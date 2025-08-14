#!/usr/bin/env python3
"""
High-Performance HTTP Server Implementations

This script implements three different server architectures:
1. Thread-based server
2. Process-based server  
3. Async I/O server

Each demonstrates the concepts learned throughout the course.
"""

import socket
import threading
import multiprocessing as mp
import asyncio
import aiohttp
import time
import json
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from urllib.parse import urlparse, parse_qs
import psutil
import signal

# Shared configuration
SERVER_CONFIG = {
    'host': '127.0.0.1',
    'port': 8080,
    'backlog': 128,
    'buffer_size': 65536,
    'worker_threads': 10,
    'worker_processes': 4,
    'max_connections': 1000
}

class BaseHTTPServer:
    """Base class for HTTP server implementations."""
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.running = False
        self.stats = {
            'requests': 0,
            'errors': 0,
            'bytes_sent': 0,
            'bytes_received': 0,
            'start_time': time.time()
        }
    
    def parse_request(self, data):
        """Parse HTTP request."""
        try:
            lines = data.decode('utf-8').split('\r\n')
            if not lines:
                return None
            
            # Parse request line
            request_line = lines[0].split(' ')
            if len(request_line) < 3:
                return None
            
            method, path, version = request_line
            
            # Parse headers
            headers = {}
            for line in lines[1:]:
                if not line:
                    break
                if ':' in line:
                    key, value = line.split(':', 1)
                    headers[key.strip().lower()] = value.strip()
            
            return {
                'method': method,
                'path': path,
                'version': version,
                'headers': headers
            }
        except Exception:
            return None
    
    def build_response(self, status=200, body='', content_type='text/plain'):
        """Build HTTP response."""
        status_text = {200: 'OK', 404: 'Not Found', 500: 'Internal Server Error'}
        
        response = f"HTTP/1.1 {status} {status_text.get(status, 'Unknown')}\r\n"
        response += f"Content-Type: {content_type}\r\n"
        response += f"Content-Length: {len(body)}\r\n"
        response += f"Server: HighPerfServer/1.0\r\n"
        response += f"Date: {datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')}\r\n"
        response += "Connection: keep-alive\r\n"
        response += "\r\n"
        response += body
        
        return response.encode('utf-8')
    
    def handle_request(self, request):
        """Route and handle HTTP request."""
        if not request:
            return self.build_response(400, "Bad Request")
        
        path = request['path']
        
        # Routing
        if path == '/':
            body = json.dumps({
                'message': 'High-Performance Server',
                'type': self.__class__.__name__,
                'timestamp': datetime.now().isoformat()
            })
            return self.build_response(200, body, 'application/json')
        
        elif path == '/stats':
            uptime = time.time() - self.stats['start_time']
            stats = self.stats.copy()
            stats['uptime'] = uptime
            stats['requests_per_second'] = stats['requests'] / uptime if uptime > 0 else 0
            
            body = json.dumps(stats)
            return self.build_response(200, body, 'application/json')
        
        elif path.startswith('/compute'):
            # CPU-intensive endpoint
            n = 10000
            result = sum(i * i for i in range(n))
            body = json.dumps({'result': result, 'n': n})
            return self.build_response(200, body, 'application/json')
        
        elif path.startswith('/io'):
            # I/O simulation endpoint
            time.sleep(0.1)  # Simulate I/O
            body = json.dumps({'status': 'io_complete'})
            return self.build_response(200, body, 'application/json')
        
        else:
            return self.build_response(404, "Not Found")
    
    def update_stats(self, bytes_received, bytes_sent, error=False):
        """Update server statistics."""
        self.stats['requests'] += 1
        self.stats['bytes_received'] += bytes_received
        self.stats['bytes_sent'] += bytes_sent
        if error:
            self.stats['errors'] += 1

class ThreadedHTTPServer(BaseHTTPServer):
    """Multi-threaded HTTP server implementation."""
    
    def __init__(self, host, port, max_workers=10):
        super().__init__(host, port)
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.socket = None
    
    def start(self):
        """Start the threaded server."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Set socket options for performance
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        self.socket.bind((self.host, self.port))
        self.socket.listen(SERVER_CONFIG['backlog'])
        
        self.running = True
        print(f"Threaded HTTP Server listening on {self.host}:{self.port}")
        print(f"Worker threads: {self.max_workers}")
        
        self.accept_loop()
    
    def accept_loop(self):
        """Main accept loop."""
        while self.running:
            try:
                client_socket, client_addr = self.socket.accept()
                # Handle in thread pool
                self.executor.submit(self.handle_client, client_socket, client_addr)
            except Exception as e:
                if self.running:
                    print(f"Accept error: {e}")
    
    def handle_client(self, client_socket, client_addr):
        """Handle client connection in thread."""
        try:
            # Set socket timeout
            client_socket.settimeout(30)
            
            # Keep-alive loop
            while self.running:
                # Receive request
                data = client_socket.recv(SERVER_CONFIG['buffer_size'])
                if not data:
                    break
                
                # Parse and handle request
                request = self.parse_request(data)
                response = self.handle_request(request)
                
                # Send response
                client_socket.sendall(response)
                
                # Update stats
                self.update_stats(len(data), len(response))
                
                # Check if keep-alive
                if request and request['headers'].get('connection', '').lower() == 'close':
                    break
                    
        except socket.timeout:
            pass
        except Exception as e:
            self.update_stats(0, 0, error=True)
        finally:
            client_socket.close()
    
    def stop(self):
        """Stop the server."""
        self.running = False
        if self.socket:
            self.socket.close()
        self.executor.shutdown(wait=True)

class MultiprocessHTTPServer(BaseHTTPServer):
    """Multi-process HTTP server implementation."""
    
    def __init__(self, host, port, num_workers=4):
        super().__init__(host, port)
        self.num_workers = num_workers
        self.workers = []
        self.socket = None
    
    def start(self):
        """Start the multi-process server."""
        # Create and bind socket in parent
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.socket.bind((self.host, self.port))
        self.socket.listen(SERVER_CONFIG['backlog'])
        
        print(f"Multiprocess HTTP Server listening on {self.host}:{self.port}")
        print(f"Worker processes: {self.num_workers}")
        
        # Fork workers
        for i in range(self.num_workers):
            pid = os.fork()
            if pid == 0:  # Child process
                self.worker_loop(i)
                os._exit(0)
            else:  # Parent process
                self.workers.append(pid)
        
        # Parent waits for workers
        self.running = True
        
        # Handle signals
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Wait for workers
        for pid in self.workers:
            os.waitpid(pid, 0)
    
    def worker_loop(self, worker_id):
        """Worker process loop."""
        print(f"Worker {worker_id} (PID {os.getpid()}) started")
        
        while True:
            try:
                client_socket, client_addr = self.socket.accept()
                self.handle_client(client_socket, client_addr)
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
    
    def handle_client(self, client_socket, client_addr):
        """Handle client connection in process."""
        try:
            client_socket.settimeout(30)
            
            # Receive request
            data = client_socket.recv(SERVER_CONFIG['buffer_size'])
            if data:
                # Parse and handle request
                request = self.parse_request(data)
                response = self.handle_request(request)
                
                # Send response
                client_socket.sendall(response)
                
                # Update stats (would need shared memory for real stats)
                self.update_stats(len(data), len(response))
                
        except Exception:
            self.update_stats(0, 0, error=True)
        finally:
            client_socket.close()
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        print("\nShutting down workers...")
        for pid in self.workers:
            try:
                os.kill(pid, signal.SIGTERM)
            except:
                pass
        sys.exit(0)

class AsyncHTTPServer(BaseHTTPServer):
    """Async I/O HTTP server implementation."""
    
    def __init__(self, host, port):
        super().__init__(host, port)
        self.server = None
    
    async def start(self):
        """Start the async server."""
        self.running = True
        
        # Create server
        self.server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port,
            backlog=SERVER_CONFIG['backlog']
        )
        
        print(f"Async HTTP Server listening on {self.host}:{self.port}")
        print(f"Event loop: {type(asyncio.get_event_loop()).__name__}")
        
        async with self.server:
            await self.server.serve_forever()
    
    async def handle_client(self, reader, writer):
        """Handle client connection asynchronously."""
        try:
            # Read request
            data = await reader.read(SERVER_CONFIG['buffer_size'])
            if not data:
                return
            
            # Parse request
            request = self.parse_request(data)
            
            # Handle request (simulate async I/O for some endpoints)
            if request and request['path'].startswith('/io'):
                await asyncio.sleep(0.1)  # Async I/O simulation
            
            response = self.handle_request(request)
            
            # Send response
            writer.write(response)
            await writer.drain()
            
            # Update stats
            self.update_stats(len(data), len(response))
            
        except Exception:
            self.update_stats(0, 0, error=True)
        finally:
            writer.close()
            await writer.wait_closed()

def benchmark_server(server_type='threaded'):
    """Run a specific server type."""
    host = SERVER_CONFIG['host']
    port = SERVER_CONFIG['port']
    
    if server_type == 'threaded':
        server = ThreadedHTTPServer(host, port, max_workers=SERVER_CONFIG['worker_threads'])
        server.start()
        
    elif server_type == 'multiprocess':
        server = MultiprocessHTTPServer(host, port, num_workers=SERVER_CONFIG['worker_processes'])
        server.start()
        
    elif server_type == 'async':
        server = AsyncHTTPServer(host, port)
        asyncio.run(server.start())
    
    else:
        print(f"Unknown server type: {server_type}")

def show_system_info():
    """Display system information."""
    print("\nSystem Information:")
    print(f"  CPU cores: {mp.cpu_count()}")
    print(f"  Python version: {sys.version.split()[0]}")
    print(f"  Platform: {sys.platform}")
    
    process = psutil.Process()
    print(f"  Process memory: {process.memory_info().rss / 1024 / 1024:.1f} MB")
    
    # File descriptor limit
    if hasattr(os, 'sysconf'):
        try:
            fd_limit = os.sysconf('SC_OPEN_MAX')
            print(f"  File descriptor limit: {fd_limit}")
        except:
            pass

def main():
    """Run the high-performance HTTP server."""
    print("High-Performance HTTP Server")
    print("=" * 60)
    
    show_system_info()
    
    if len(sys.argv) > 1:
        server_type = sys.argv[1]
    else:
        print("\nSelect server type:")
        print("1. Threaded")
        print("2. Multiprocess")
        print("3. Async")
        
        choice = input("Enter choice (1-3): ").strip()
        server_type = {'1': 'threaded', '2': 'multiprocess', '3': 'async'}.get(choice, 'threaded')
    
    print(f"\nStarting {server_type} server...")
    print(f"Server URL: http://{SERVER_CONFIG['host']}:{SERVER_CONFIG['port']}/")
    print("\nEndpoints:")
    print("  /        - Server info")
    print("  /stats   - Server statistics")
    print("  /compute - CPU-intensive task")
    print("  /io      - I/O-intensive task")
    print("\nPress Ctrl+C to stop\n")
    
    try:
        benchmark_server(server_type)
    except KeyboardInterrupt:
        print("\nServer stopped")

if __name__ == "__main__":
    main()
