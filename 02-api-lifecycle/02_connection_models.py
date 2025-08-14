#!/usr/bin/env python3
"""
Connection Handling Models: Comparing different server architectures

This script implements and compares different connection handling models:
- Thread per connection
- Process per connection (fork)
- Event-driven (select/epoll)
- Thread pool
"""

import socket
import threading
import multiprocessing
import selectors
import time
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import signal
import json

class ConnectionModelDemo:
    """Base class for connection model demonstrations."""
    
    def __init__(self, name, port):
        self.name = name
        self.port = port
        self.host = '127.0.0.1'
        self.running = False
        self.connections_handled = 0
        self.start_time = None
        
    def create_server_socket(self):
        """Create and configure server socket."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(128)
        return sock
    
    def handle_request(self, client_socket, client_addr):
        """Handle a single HTTP request."""
        try:
            # Receive request
            data = client_socket.recv(4096)
            if not data:
                return
            
            # Parse basic request info
            request_line = data.decode('utf-8').split('\r\n')[0]
            method, path, _ = request_line.split(' ')
            
            # Prepare response based on path
            if path == '/info':
                response_body = self.get_info_response()
            else:
                response_body = f"Hello from {self.name} model!"
            
            # Send HTTP response
            response = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/plain\r\n"
                f"Content-Length: {len(response_body)}\r\n"
                "Connection: close\r\n"
                "\r\n"
                f"{response_body}"
            ).encode('utf-8')
            
            client_socket.sendall(response)
            self.connections_handled += 1
            
        except Exception as e:
            print(f"[{self.name}] Error handling request: {e}")
        finally:
            client_socket.close()
    
    def get_info_response(self):
        """Get server information response."""
        uptime = time.time() - self.start_time if self.start_time else 0
        info = {
            "model": self.name,
            "pid": os.getpid(),
            "connections_handled": self.connections_handled,
            "uptime_seconds": round(uptime, 2),
            "thread_count": threading.active_count(),
            "timestamp": datetime.now().isoformat()
        }
        return json.dumps(info, indent=2)
    
    def start(self):
        """Start the server (to be implemented by subclasses)."""
        raise NotImplementedError
    
    def stop(self):
        """Stop the server."""
        self.running = False

class ThreadPerConnectionServer(ConnectionModelDemo):
    """Server that creates a new thread for each connection."""
    
    def __init__(self, port):
        super().__init__("Thread-per-Connection", port)
        self.threads = []
    
    def start(self):
        """Start the thread-per-connection server."""
        self.server_socket = self.create_server_socket()
        self.running = True
        self.start_time = time.time()
        
        print(f"[{self.name}] Server started on port {self.port}")
        
        while self.running:
            try:
                client_socket, client_addr = self.server_socket.accept()
                
                # Create new thread for this connection
                thread = threading.Thread(
                    target=self.handle_request,
                    args=(client_socket, client_addr),
                    name=f"Worker-{len(self.threads)}"
                )
                thread.daemon = True
                thread.start()
                self.threads.append(thread)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                if self.running:
                    print(f"[{self.name}] Accept error: {e}")
        
        self.server_socket.close()

class ProcessPerConnectionServer(ConnectionModelDemo):
    """Server that creates a new process for each connection (fork model)."""
    
    def __init__(self, port):
        super().__init__("Process-per-Connection", port)
        self.children = []
    
    def start(self):
        """Start the process-per-connection server."""
        self.server_socket = self.create_server_socket()
        self.running = True
        self.start_time = time.time()
        
        print(f"[{self.name}] Server started on port {self.port}")
        
        # Handle SIGCHLD to reap zombie processes
        signal.signal(signal.SIGCHLD, self._reap_children)
        
        while self.running:
            try:
                client_socket, client_addr = self.server_socket.accept()
                
                # Fork a new process
                pid = os.fork()
                
                if pid == 0:  # Child process
                    self.server_socket.close()  # Close parent's socket
                    self.handle_request(client_socket, client_addr)
                    os._exit(0)
                else:  # Parent process
                    client_socket.close()  # Close child's socket
                    self.children.append(pid)
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                if self.running:
                    print(f"[{self.name}] Accept error: {e}")
        
        # Clean up
        for pid in self.children:
            try:
                os.kill(pid, signal.SIGTERM)
            except:
                pass
        
        self.server_socket.close()
    
    def _reap_children(self, signum, frame):
        """Reap zombie child processes."""
        while True:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
                if pid == 0:
                    break
                if pid in self.children:
                    self.children.remove(pid)
            except:
                break

class EventDrivenServer(ConnectionModelDemo):
    """Event-driven server using selectors (single thread)."""
    
    def __init__(self, port):
        super().__init__("Event-Driven", port)
        self.selector = selectors.DefaultSelector()
        self.client_buffers = {}
    
    def start(self):
        """Start the event-driven server."""
        self.server_socket = self.create_server_socket()
        self.server_socket.setblocking(False)
        self.running = True
        self.start_time = time.time()
        
        # Register server socket for accept events
        self.selector.register(self.server_socket, selectors.EVENT_READ, data=None)
        
        print(f"[{self.name}] Server started on port {self.port}")
        print(f"[{self.name}] Using selector: {type(self.selector).__name__}")
        
        while self.running:
            try:
                # Wait for events
                events = self.selector.select(timeout=1)
                
                for key, mask in events:
                    if key.data is None:
                        # Accept new connection
                        self._accept_connection(key.fileobj)
                    else:
                        # Handle client socket
                        self._handle_client_event(key, mask)
                        
            except KeyboardInterrupt:
                break
        
        self.selector.close()
        self.server_socket.close()
    
    def _accept_connection(self, server_socket):
        """Accept a new connection."""
        try:
            client_socket, client_addr = server_socket.accept()
            client_socket.setblocking(False)
            
            # Register client socket for read events
            data = {'addr': client_addr, 'inbound': b'', 'outbound': b''}
            self.selector.register(client_socket, selectors.EVENT_READ, data=data)
            
        except Exception as e:
            print(f"[{self.name}] Accept error: {e}")
    
    def _handle_client_event(self, key, mask):
        """Handle events on client sockets."""
        client_socket = key.fileobj
        data = key.data
        
        if mask & selectors.EVENT_READ:
            # Read data from client
            try:
                recv_data = client_socket.recv(4096)
                if recv_data:
                    data['inbound'] += recv_data
                    
                    # Check if we have a complete request
                    if b'\r\n\r\n' in data['inbound']:
                        # Process request
                        request_line = data['inbound'].decode('utf-8').split('\r\n')[0]
                        method, path, _ = request_line.split(' ')
                        
                        # Prepare response
                        if path == '/info':
                            response_body = self.get_info_response()
                        else:
                            response_body = f"Hello from {self.name} model!"
                        
                        response = (
                            "HTTP/1.1 200 OK\r\n"
                            "Content-Type: text/plain\r\n"
                            f"Content-Length: {len(response_body)}\r\n"
                            "Connection: close\r\n"
                            "\r\n"
                            f"{response_body}"
                        ).encode('utf-8')
                        
                        data['outbound'] = response
                        self.connections_handled += 1
                        
                        # Switch to write mode
                        self.selector.modify(client_socket, selectors.EVENT_WRITE, data=data)
                else:
                    # Connection closed by client
                    self.selector.unregister(client_socket)
                    client_socket.close()
                    
            except Exception as e:
                print(f"[{self.name}] Read error: {e}")
                self.selector.unregister(client_socket)
                client_socket.close()
        
        elif mask & selectors.EVENT_WRITE:
            # Write data to client
            if data['outbound']:
                try:
                    sent = client_socket.send(data['outbound'])
                    data['outbound'] = data['outbound'][sent:]
                    
                    # If all data sent, close connection
                    if not data['outbound']:
                        self.selector.unregister(client_socket)
                        client_socket.close()
                        
                except Exception as e:
                    print(f"[{self.name}] Write error: {e}")
                    self.selector.unregister(client_socket)
                    client_socket.close()

class ThreadPoolServer(ConnectionModelDemo):
    """Server using a thread pool for handling connections."""
    
    def __init__(self, port, pool_size=10):
        super().__init__("Thread-Pool", port)
        self.pool_size = pool_size
        self.executor = None
    
    def start(self):
        """Start the thread pool server."""
        self.server_socket = self.create_server_socket()
        self.running = True
        self.start_time = time.time()
        self.executor = ThreadPoolExecutor(max_workers=self.pool_size)
        
        print(f"[{self.name}] Server started on port {self.port}")
        print(f"[{self.name}] Thread pool size: {self.pool_size}")
        
        while self.running:
            try:
                client_socket, client_addr = self.server_socket.accept()
                
                # Submit to thread pool
                self.executor.submit(self.handle_request, client_socket, client_addr)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                if self.running:
                    print(f"[{self.name}] Accept error: {e}")
        
        self.executor.shutdown(wait=True)
        self.server_socket.close()

def run_performance_test(server_class, port, duration=5):
    """Run a simple performance test on a server."""
    print(f"\n{'='*60}")
    print(f"Testing {server_class.__name__}")
    print(f"{'='*60}")
    
    # Start server in a separate process
    server_process = multiprocessing.Process(target=lambda: server_class(port).start())
    server_process.start()
    
    # Wait for server to start
    time.sleep(1)
    
    # Run test
    start_time = time.time()
    connections = 0
    errors = 0
    
    print(f"Running {duration} second test...")
    
    while time.time() - start_time < duration:
        try:
            # Create connection
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('127.0.0.1', port))
            
            # Send request
            sock.send(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            
            # Receive response
            response = sock.recv(4096)
            sock.close()
            
            connections += 1
            
        except Exception as e:
            errors += 1
    
    # Get server info
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', port))
        sock.send(b"GET /info HTTP/1.1\r\nHost: localhost\r\n\r\n")
        info_response = sock.recv(4096)
        sock.close()
        
        # Parse JSON from response
        json_start = info_response.find(b'{')
        if json_start != -1:
            info = json.loads(info_response[json_start:])
            print(f"\nServer Info:")
            for key, value in info.items():
                print(f"  {key}: {value}")
    except:
        pass
    
    # Stop server
    server_process.terminate()
    server_process.join()
    
    # Print results
    total_time = time.time() - start_time
    print(f"\nResults:")
    print(f"  Total connections: {connections}")
    print(f"  Errors: {errors}")
    print(f"  Connections/second: {connections/total_time:.1f}")
    print(f"  Average latency: {total_time/connections*1000:.1f} ms")

def main():
    """Run connection model demonstrations."""
    print("Connection Handling Models Comparison")
    print("=====================================\n")
    
    # Check if we're on a Unix-like system for fork support
    can_fork = hasattr(os, 'fork')
    
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        # Run performance tests
        base_port = 8080
        
        servers = [
            (ThreadPerConnectionServer, base_port),
            (ThreadPoolServer, base_port + 1),
            (EventDrivenServer, base_port + 2),
        ]
        
        if can_fork:
            servers.append((ProcessPerConnectionServer, base_port + 3))
        
        for server_class, port in servers:
            run_performance_test(server_class, port)
            time.sleep(1)  # Brief pause between tests
            
    else:
        # Interactive demo
        print("Choose a server model to run:")
        print("1. Thread per connection")
        print("2. Thread pool")
        print("3. Event-driven")
        if can_fork:
            print("4. Process per connection (fork)")
        
        choice = input("\nEnter choice (1-4): ").strip()
        
        port = 8080
        server = None
        
        if choice == '1':
            server = ThreadPerConnectionServer(port)
        elif choice == '2':
            server = ThreadPoolServer(port)
        elif choice == '3':
            server = EventDrivenServer(port)
        elif choice == '4' and can_fork:
            server = ProcessPerConnectionServer(port)
        else:
            print("Invalid choice")
            return
        
        print(f"\nStarting {server.name} server...")
        print(f"Test with: curl http://localhost:{port}/")
        print(f"Get info: curl http://localhost:{port}/info")
        print("Press Ctrl+C to stop\n")
        
        try:
            server.start()
        except KeyboardInterrupt:
            print(f"\n{server.name} server stopped")

if __name__ == "__main__":
    main()
