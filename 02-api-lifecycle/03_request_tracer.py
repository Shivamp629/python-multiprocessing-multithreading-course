#!/usr/bin/env python3
"""
Request Lifecycle Tracer: Detailed tracking of HTTP request processing

This script provides detailed insights into how an HTTP request flows through
the system, including system calls, buffer operations, and timing measurements.
"""

import socket
import time
import threading
import os
import sys
import struct
import fcntl
import errno
from datetime import datetime
import traceback
import psutil

# Try to import platform-specific modules
try:
    import resource
except ImportError:
    resource = None

class SystemCallTracer:
    """Traces system calls and their timing."""
    
    def __init__(self):
        self.traces = []
        self.enabled = True
    
    def trace(self, syscall_name, args, start_time, end_time, result=None, error=None):
        """Record a system call trace."""
        if self.enabled:
            self.traces.append({
                'syscall': syscall_name,
                'args': args,
                'start': start_time,
                'duration_us': (end_time - start_time) * 1_000_000,
                'result': result,
                'error': error,
                'thread_id': threading.current_thread().ident
            })
    
    def get_summary(self):
        """Get summary of system calls."""
        if not self.traces:
            return {}
        
        summary = {}
        for trace in self.traces:
            syscall = trace['syscall']
            if syscall not in summary:
                summary[syscall] = {
                    'count': 0,
                    'total_time_us': 0,
                    'min_time_us': float('inf'),
                    'max_time_us': 0
                }
            
            summary[syscall]['count'] += 1
            summary[syscall]['total_time_us'] += trace['duration_us']
            summary[syscall]['min_time_us'] = min(summary[syscall]['min_time_us'], trace['duration_us'])
            summary[syscall]['max_time_us'] = max(summary[syscall]['max_time_us'], trace['duration_us'])
        
        # Calculate averages
        for syscall in summary:
            summary[syscall]['avg_time_us'] = summary[syscall]['total_time_us'] / summary[syscall]['count']
        
        return summary

class TracedSocket:
    """Socket wrapper that traces system calls."""
    
    def __init__(self, family=socket.AF_INET, type=socket.SOCK_STREAM, tracer=None):
        self.tracer = tracer or SystemCallTracer()
        self._socket = None
        
        # Create socket with tracing
        start = time.perf_counter()
        self._socket = socket.socket(family, type)
        end = time.perf_counter()
        
        self.tracer.trace('socket', f"family={family}, type={type}", start, end, 
                         result=f"fd={self._socket.fileno()}")
    
    def __getattr__(self, name):
        """Delegate to underlying socket with tracing."""
        attr = getattr(self._socket, name)
        
        if callable(attr) and name in ['bind', 'listen', 'accept', 'recv', 'send', 
                                       'connect', 'close', 'setsockopt', 'getsockopt']:
            def traced_method(*args, **kwargs):
                start = time.perf_counter()
                error = None
                result = None
                
                try:
                    result = attr(*args, **kwargs)
                    end = time.perf_counter()
                    
                    # Format result for logging
                    if name == 'accept':
                        if result:
                            conn, addr = result
                            result_str = f"fd={conn.fileno()}, addr={addr}"
                        else:
                            result_str = "None"
                    elif name == 'recv':
                        result_str = f"{len(result)} bytes" if result else "0 bytes"
                    elif name == 'send':
                        result_str = f"{result} bytes sent"
                    else:
                        result_str = str(result)
                    
                    self.tracer.trace(name, str(args[:2] if args else ''), start, end, result=result_str)
                    return result
                    
                except Exception as e:
                    end = time.perf_counter()
                    error = str(e)
                    self.tracer.trace(name, str(args[:2] if args else ''), start, end, error=error)
                    raise
            
            return traced_method
        return attr

class BufferMonitor:
    """Monitors socket buffer sizes and usage."""
    
    @staticmethod
    def get_socket_buffers(sock):
        """Get current socket buffer sizes."""
        try:
            recv_buf = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
            send_buf = sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
            
            # Try to get actual buffer usage (Linux-specific)
            if hasattr(socket, 'SO_RCVQ') and hasattr(socket, 'SO_SNDQ'):
                try:
                    recv_queued = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVQ)
                    send_queued = sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDQ)
                except:
                    recv_queued = send_queued = None
            else:
                recv_queued = send_queued = None
            
            return {
                'recv_buffer_size': recv_buf,
                'send_buffer_size': send_buf,
                'recv_queued': recv_queued,
                'send_queued': send_queued
            }
        except:
            return None

class RequestLifecycleTracer:
    """Comprehensive HTTP request lifecycle tracer."""
    
    def __init__(self, port=8080):
        self.port = port
        self.host = '127.0.0.1'
        self.syscall_tracer = SystemCallTracer()
        self.request_stages = []
        self.memory_snapshots = []
    
    def take_memory_snapshot(self, stage):
        """Take a snapshot of current memory usage."""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            
            snapshot = {
                'stage': stage,
                'timestamp': time.perf_counter(),
                'rss_mb': memory_info.rss / 1024 / 1024,
                'vms_mb': memory_info.vms / 1024 / 1024,
            }
            
            # Get more detailed info if available
            if hasattr(process, 'memory_full_info'):
                full_info = process.memory_full_info()
                snapshot['uss_mb'] = full_info.uss / 1024 / 1024  # Unique Set Size
            
            self.memory_snapshots.append(snapshot)
        except:
            pass
    
    def log_stage(self, stage_name, details=None):
        """Log a request processing stage."""
        self.request_stages.append({
            'stage': stage_name,
            'timestamp': time.perf_counter(),
            'thread_id': threading.current_thread().ident,
            'details': details or {}
        })
        
        # Take memory snapshot at key stages
        if stage_name in ['server_started', 'connection_accepted', 'request_received', 'response_sent']:
            self.take_memory_snapshot(stage_name)
    
    def start_server(self):
        """Start the traced HTTP server."""
        print("Starting Request Lifecycle Tracer Server")
        print("=" * 60)
        
        # Create traced socket
        server_socket = TracedSocket(tracer=self.syscall_tracer)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Get initial buffer sizes
        initial_buffers = BufferMonitor.get_socket_buffers(server_socket)
        print(f"\nInitial socket buffers:")
        if initial_buffers:
            print(f"  Receive buffer: {initial_buffers['recv_buffer_size']:,} bytes")
            print(f"  Send buffer: {initial_buffers['send_buffer_size']:,} bytes")
        
        # Bind and listen
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        
        print(f"\nServer listening on {self.host}:{self.port}")
        print(f"PID: {os.getpid()}")
        print(f"Socket FD: {server_socket.fileno()}")
        
        self.log_stage('server_started', {
            'pid': os.getpid(),
            'socket_fd': server_socket.fileno(),
            'buffers': initial_buffers
        })
        
        print("\nWaiting for connection...")
        print("Test with: curl -v http://localhost:8080/trace\n")
        
        try:
            # Accept one connection for detailed tracing
            self.handle_single_request(server_socket)
        finally:
            server_socket.close()
    
    def handle_single_request(self, server_socket):
        """Handle a single request with detailed tracing."""
        # Accept connection
        print("Accepting connection...")
        conn_socket, client_addr = server_socket.accept()
        conn_wrapped = TracedSocket(tracer=self.syscall_tracer)
        conn_wrapped._socket = conn_socket
        
        self.log_stage('connection_accepted', {
            'client_addr': client_addr,
            'conn_fd': conn_socket.fileno()
        })
        
        print(f"Connection from {client_addr}")
        print(f"Connection socket FD: {conn_socket.fileno()}")
        
        # Get connection buffer info
        conn_buffers = BufferMonitor.get_socket_buffers(conn_socket)
        if conn_buffers:
            print(f"\nConnection buffers:")
            print(f"  Receive buffer: {conn_buffers['recv_buffer_size']:,} bytes")
            print(f"  Send buffer: {conn_buffers['send_buffer_size']:,} bytes")
        
        try:
            # Receive request
            print("\nReceiving request...")
            request_data = b''
            
            while True:
                chunk = conn_wrapped.recv(4096)
                if not chunk:
                    break
                    
                request_data += chunk
                
                # Check for end of headers
                if b'\r\n\r\n' in request_data:
                    break
            
            self.log_stage('request_received', {
                'bytes_received': len(request_data),
                'buffers': conn_buffers
            })
            
            print(f"Received {len(request_data)} bytes")
            
            # Parse request
            request_lines = request_data.decode('utf-8').split('\r\n')
            request_line = request_lines[0] if request_lines else ''
            print(f"Request: {request_line}")
            
            # Process request
            self.log_stage('processing_request')
            
            # Simulate some processing
            time.sleep(0.01)  # 10ms processing
            
            # Prepare response
            response_body = self.generate_trace_report()
            response_headers = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/plain\r\n"
                f"Content-Length: {len(response_body)}\r\n"
                "Connection: close\r\n"
                "\r\n"
            )
            response = response_headers.encode('utf-8') + response_body.encode('utf-8')
            
            self.log_stage('sending_response', {
                'response_size': len(response)
            })
            
            # Send response
            print(f"\nSending response ({len(response)} bytes)...")
            total_sent = 0
            
            while total_sent < len(response):
                sent = conn_wrapped.send(response[total_sent:])
                total_sent += sent
                print(f"  Sent {sent} bytes (total: {total_sent}/{len(response)})")
            
            self.log_stage('response_sent')
            
        finally:
            conn_wrapped.close()
            self.log_stage('connection_closed')
    
    def generate_trace_report(self):
        """Generate a detailed trace report."""
        report = []
        report.append("REQUEST LIFECYCLE TRACE REPORT")
        report.append("=" * 60)
        report.append(f"Generated at: {datetime.now()}")
        report.append("")
        
        # Request stages timeline
        report.append("REQUEST STAGES TIMELINE:")
        report.append("-" * 40)
        
        if self.request_stages:
            start_time = self.request_stages[0]['timestamp']
            
            for stage in self.request_stages:
                elapsed_ms = (stage['timestamp'] - start_time) * 1000
                report.append(f"{elapsed_ms:>8.2f} ms  {stage['stage']}")
                
                if stage['details']:
                    for key, value in stage['details'].items():
                        report.append(f"              {key}: {value}")
        
        # System calls summary
        report.append("\n\nSYSTEM CALLS SUMMARY:")
        report.append("-" * 40)
        
        syscall_summary = self.syscall_tracer.get_summary()
        if syscall_summary:
            report.append(f"{'Syscall':<15} {'Count':>8} {'Avg μs':>10} {'Min μs':>10} {'Max μs':>10} {'Total μs':>12}")
            report.append("-" * 70)
            
            for syscall, stats in sorted(syscall_summary.items()):
                report.append(
                    f"{syscall:<15} {stats['count']:>8} "
                    f"{stats['avg_time_us']:>10.1f} "
                    f"{stats['min_time_us']:>10.1f} "
                    f"{stats['max_time_us']:>10.1f} "
                    f"{stats['total_time_us']:>12.1f}"
                )
        
        # Memory usage
        report.append("\n\nMEMORY USAGE:")
        report.append("-" * 40)
        
        if self.memory_snapshots:
            report.append(f"{'Stage':<20} {'RSS (MB)':>10} {'VMS (MB)':>10} {'Delta RSS':>10}")
            report.append("-" * 50)
            
            prev_rss = None
            for snapshot in self.memory_snapshots:
                delta = f"{snapshot['rss_mb'] - prev_rss:+.1f}" if prev_rss else "---"
                report.append(
                    f"{snapshot['stage']:<20} "
                    f"{snapshot['rss_mb']:>10.1f} "
                    f"{snapshot['vms_mb']:>10.1f} "
                    f"{delta:>10}"
                )
                prev_rss = snapshot['rss_mb']
        
        # System call traces (detailed)
        report.append("\n\nDETAILED SYSTEM CALL TRACES:")
        report.append("-" * 40)
        
        for i, trace in enumerate(self.syscall_tracer.traces[:20]):  # First 20 traces
            report.append(
                f"{i+1:>3}. {trace['syscall']}({trace['args']}) "
                f"-> {trace.get('result', trace.get('error', 'unknown'))} "
                f"[{trace['duration_us']:.1f} μs]"
            )
        
        if len(self.syscall_tracer.traces) > 20:
            report.append(f"... and {len(self.syscall_tracer.traces) - 20} more traces")
        
        return '\n'.join(report)

class ClientTracer:
    """Trace client-side of the connection."""
    
    def __init__(self, host='127.0.0.1', port=8080):
        self.host = host
        self.port = port
        self.tracer = SystemCallTracer()
    
    def make_request(self):
        """Make a traced HTTP request."""
        print("\nCLIENT-SIDE TRACE")
        print("=" * 40)
        
        # Create traced socket
        client_socket = TracedSocket(tracer=self.tracer)
        
        try:
            # Connect
            print(f"Connecting to {self.host}:{self.port}...")
            start_connect = time.perf_counter()
            client_socket.connect((self.host, self.port))
            connect_time = (time.perf_counter() - start_connect) * 1000
            print(f"Connected in {connect_time:.1f} ms")
            
            # Send request
            request = (
                "GET /trace HTTP/1.1\r\n"
                f"Host: {self.host}\r\n"
                "User-Agent: RequestTracer/1.0\r\n"
                "Accept: */*\r\n"
                "\r\n"
            ).encode('utf-8')
            
            print(f"\nSending request ({len(request)} bytes)...")
            client_socket.send(request)
            
            # Receive response
            print("Receiving response...")
            response = b''
            
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                response += chunk
            
            print(f"Received {len(response)} bytes")
            
            # Parse response
            if response:
                header_end = response.find(b'\r\n\r\n')
                if header_end > 0:
                    headers = response[:header_end].decode('utf-8')
                    status_line = headers.split('\r\n')[0]
                    print(f"Response: {status_line}")
            
        finally:
            client_socket.close()
        
        # Print client-side system call summary
        print("\nClient-side system calls:")
        summary = self.tracer.get_summary()
        for syscall, stats in summary.items():
            print(f"  {syscall}: {stats['count']} calls, avg {stats['avg_time_us']:.1f} μs")

def main():
    """Run the request lifecycle tracer."""
    if len(sys.argv) > 1 and sys.argv[1] == 'client':
        # Run as client
        tracer = ClientTracer()
        tracer.make_request()
    else:
        # Run as server
        tracer = RequestLifecycleTracer()
        
        try:
            tracer.start_server()
        except KeyboardInterrupt:
            print("\n\nServer stopped")
        except Exception as e:
            print(f"\nError: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    main()
