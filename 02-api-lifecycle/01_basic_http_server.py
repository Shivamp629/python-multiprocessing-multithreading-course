#!/usr/bin/env python3
"""
Basic HTTP Server: Understanding Request Lifecycle

This script implements a simple HTTP server to demonstrate how requests
are received, processed, and responded to at the system level.
"""

import socket
import threading
import time
import json
import sys
import os
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import traceback

class HTTPRequest:
    """Represents an HTTP request."""
    
    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.method = None
        self.path = None
        self.version = None
        self.headers = {}
        self.body = b''
        self.parse()
    
    def parse(self):
        """Parse the raw HTTP request."""
        try:
            # Split headers and body
            parts = self.raw_data.split(b'\r\n\r\n', 1)
            header_data = parts[0]
            if len(parts) > 1:
                self.body = parts[1]
            
            # Parse request line and headers
            lines = header_data.decode('utf-8').split('\r\n')
            if lines:
                # Parse request line
                request_parts = lines[0].split(' ')
                if len(request_parts) >= 3:
                    self.method = request_parts[0]
                    self.path = request_parts[1]
                    self.version = request_parts[2]
                
                # Parse headers
                for line in lines[1:]:
                    if ':' in line:
                        key, value = line.split(':', 1)
                        self.headers[key.strip().lower()] = value.strip()
        except Exception as e:
            print(f"Error parsing request: {e}")

class HTTPResponse:
    """Helper to build HTTP responses."""
    
    @staticmethod
    def create(status_code, status_text, body, content_type='text/plain'):
        """Create an HTTP response."""
        if isinstance(body, str):
            body = body.encode('utf-8')
        
        headers = [
            f"HTTP/1.1 {status_code} {status_text}",
            f"Content-Type: {content_type}",
            f"Content-Length: {len(body)}",
            f"Connection: close",
            f"Server: PythonDemo/1.0",
            f"Date: {datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')}"
        ]
        
        response = '\r\n'.join(headers) + '\r\n\r\n'
        return response.encode('utf-8') + body

class RequestLifecycleTracker:
    """Tracks the lifecycle of requests through the server."""
    
    def __init__(self):
        self.events = []
        self.lock = threading.Lock()
    
    def log_event(self, request_id, event, details=None):
        """Log a lifecycle event."""
        with self.lock:
            self.events.append({
                'request_id': request_id,
                'timestamp': time.perf_counter(),
                'event': event,
                'thread_id': threading.current_thread().ident,
                'details': details
            })
    
    def get_request_timeline(self, request_id):
        """Get timeline for a specific request."""
        with self.lock:
            return [e for e in self.events if e['request_id'] == request_id]

class SimpleHTTPServer:
    """A simple HTTP server demonstrating request lifecycle."""
    
    def __init__(self, host='127.0.0.1', port=8080):
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        self.request_count = 0
        self.lifecycle_tracker = RequestLifecycleTracker()
        self.thread_pool = []
    
    def start(self):
        """Start the HTTP server."""
        # Create and configure socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind and listen
        self.socket.bind((self.host, self.port))
        self.socket.listen(128)  # Backlog queue size
        
        print(f"HTTP Server started on http://{self.host}:{self.port}")
        print(f"Process ID: {os.getpid()}")
        print(f"Main thread ID: {threading.current_thread().ident}")
        print("\nEndpoints:")
        print("  /         - Basic info")
        print("  /slow     - Simulate slow processing")
        print("  /lifecycle - Show request lifecycle")
        print("  /stats    - Server statistics")
        print("\nPress Ctrl+C to stop\n")
        
        self.running = True
        self.accept_connections()
    
    def accept_connections(self):
        """Accept incoming connections."""
        while self.running:
            try:
                # Accept new connection
                client_socket, client_addr = self.socket.accept()
                self.request_count += 1
                request_id = f"req_{self.request_count}"
                
                # Log connection accepted
                self.lifecycle_tracker.log_event(
                    request_id, 
                    'connection_accepted',
                    {'client_addr': client_addr}
                )
                
                # Handle in new thread
                thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_addr, request_id)
                )
                thread.daemon = True
                thread.start()
                self.thread_pool.append(thread)
                
            except KeyboardInterrupt:
                print("\nShutting down server...")
                self.running = False
            except Exception as e:
                print(f"Error accepting connection: {e}")
    
    def handle_client(self, client_socket, client_addr, request_id):
        """Handle a client connection."""
        try:
            # Log handler start
            self.lifecycle_tracker.log_event(request_id, 'handler_started')
            
            # Receive request data
            start_recv = time.perf_counter()
            request_data = self.receive_request(client_socket)
            recv_time = time.perf_counter() - start_recv
            
            self.lifecycle_tracker.log_event(
                request_id, 
                'request_received',
                {'recv_time_ms': recv_time * 1000, 'bytes': len(request_data)}
            )
            
            if not request_data:
                return
            
            # Parse request
            start_parse = time.perf_counter()
            request = HTTPRequest(request_data)
            parse_time = time.perf_counter() - start_parse
            
            self.lifecycle_tracker.log_event(
                request_id,
                'request_parsed',
                {'parse_time_ms': parse_time * 1000, 'method': request.method, 'path': request.path}
            )
            
            # Process request
            start_process = time.perf_counter()
            response = self.route_request(request, request_id)
            process_time = time.perf_counter() - start_process
            
            self.lifecycle_tracker.log_event(
                request_id,
                'request_processed',
                {'process_time_ms': process_time * 1000}
            )
            
            # Send response
            start_send = time.perf_counter()
            client_socket.sendall(response)
            send_time = time.perf_counter() - start_send
            
            self.lifecycle_tracker.log_event(
                request_id,
                'response_sent',
                {'send_time_ms': send_time * 1000, 'bytes': len(response)}
            )
            
            # Log request
            total_time = (recv_time + parse_time + process_time + send_time) * 1000
            print(f"[{request_id}] {client_addr[0]}:{client_addr[1]} - "
                  f"{request.method} {request.path} - "
                  f"{total_time:.1f}ms total "
                  f"(recv: {recv_time*1000:.1f}ms, "
                  f"parse: {parse_time*1000:.1f}ms, "
                  f"process: {process_time*1000:.1f}ms, "
                  f"send: {send_time*1000:.1f}ms)")
            
        except Exception as e:
            print(f"Error handling client: {e}")
            traceback.print_exc()
        finally:
            client_socket.close()
            self.lifecycle_tracker.log_event(request_id, 'connection_closed')
    
    def receive_request(self, client_socket, max_size=65536):
        """Receive HTTP request data."""
        chunks = []
        received = 0
        
        while received < max_size:
            try:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                
                chunks.append(chunk)
                received += len(chunk)
                
                # Check if we have the full request
                data = b''.join(chunks)
                if b'\r\n\r\n' in data:
                    # Check for content-length
                    header_end = data.find(b'\r\n\r\n')
                    headers = data[:header_end].decode('utf-8', errors='ignore')
                    
                    content_length = 0
                    for line in headers.split('\r\n'):
                        if line.lower().startswith('content-length:'):
                            content_length = int(line.split(':')[1].strip())
                            break
                    
                    # Check if we have the full body
                    body_start = header_end + 4
                    if len(data) >= body_start + content_length:
                        break
                        
            except socket.timeout:
                break
        
        return b''.join(chunks)
    
    def route_request(self, request, request_id):
        """Route request to appropriate handler."""
        if request.path == '/':
            return self.handle_index(request)
        elif request.path == '/slow':
            return self.handle_slow(request)
        elif request.path == '/lifecycle':
            return self.handle_lifecycle(request, request_id)
        elif request.path == '/stats':
            return self.handle_stats(request)
        else:
            return HTTPResponse.create(404, 'Not Found', 'Page not found')
    
    def handle_index(self, request):
        """Handle index page."""
        body = f"""
        <html>
        <head><title>Python HTTP Server</title></head>
        <body>
            <h1>Simple HTTP Server</h1>
            <p>This server demonstrates the HTTP request lifecycle.</p>
            <ul>
                <li><a href="/slow">Slow endpoint (simulates processing)</a></li>
                <li><a href="/lifecycle">Request lifecycle details</a></li>
                <li><a href="/stats">Server statistics</a></li>
            </ul>
            <hr>
            <p>Server Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Your Request: {request.method} {request.path}</p>
        </body>
        </html>
        """
        return HTTPResponse.create(200, 'OK', body, 'text/html')
    
    def handle_slow(self, request):
        """Simulate slow processing."""
        # Parse query parameters
        parsed = urlparse(request.path)
        params = parse_qs(parsed.query)
        delay = float(params.get('delay', ['1'])[0])
        
        # Simulate processing
        time.sleep(delay)
        
        body = {
            'message': 'Slow request completed',
            'delay_seconds': delay,
            'timestamp': datetime.now().isoformat()
        }
        return HTTPResponse.create(200, 'OK', json.dumps(body), 'application/json')
    
    def handle_lifecycle(self, request, request_id):
        """Show request lifecycle information."""
        timeline = self.lifecycle_tracker.get_request_timeline(request_id)
        
        if timeline:
            start_time = timeline[0]['timestamp']
            events = []
            for event in timeline:
                events.append({
                    'event': event['event'],
                    'time_ms': (event['timestamp'] - start_time) * 1000,
                    'thread_id': event['thread_id'],
                    'details': event.get('details', {})
                })
        else:
            events = []
        
        body = {
            'request_id': request_id,
            'lifecycle_events': events,
            'total_events_tracked': len(self.lifecycle_tracker.events)
        }
        
        return HTTPResponse.create(200, 'OK', json.dumps(body, indent=2), 'application/json')
    
    def handle_stats(self, request):
        """Show server statistics."""
        active_threads = len([t for t in self.thread_pool if t.is_alive()])
        
        body = {
            'server_info': {
                'host': self.host,
                'port': self.port,
                'pid': os.getpid(),
                'main_thread_id': threading.main_thread().ident
            },
            'statistics': {
                'total_requests': self.request_count,
                'active_threads': active_threads,
                'total_threads_created': len(self.thread_pool)
            },
            'recent_events': self.lifecycle_tracker.events[-20:]  # Last 20 events
        }
        
        return HTTPResponse.create(200, 'OK', json.dumps(body, indent=2), 'application/json')

def main():
    """Run the HTTP server demonstration."""
    server = SimpleHTTPServer(port=8080)
    
    try:
        server.start()
    except KeyboardInterrupt:
        print("\nServer stopped")
    except Exception as e:
        print(f"Server error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
