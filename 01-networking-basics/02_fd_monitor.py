#!/usr/bin/env python3
"""
File Descriptor Monitor: Real-time tracking of process file descriptors

This script demonstrates how to monitor file descriptors in a process,
showing how they are created, used, and destroyed during network operations.
"""

import os
import socket
import threading
import time
import psutil
import sys
from collections import defaultdict
from datetime import datetime

class FileDescriptorMonitor:
    """Monitor and track file descriptors for a process."""
    
    def __init__(self, pid=None):
        self.pid = pid or os.getpid()
        self.process = psutil.Process(self.pid)
        self.fd_history = defaultdict(list)
        self.monitoring = False
        
    def get_open_fds(self):
        """Get all open file descriptors for the process."""
        fds = []
        try:
            # Get all open files (includes sockets)
            for item in self.process.open_files():
                fds.append({
                    'fd': item.fd,
                    'path': item.path,
                    'type': 'file'
                })
            
            # Get network connections
            for conn in self.process.connections():
                fds.append({
                    'fd': conn.fd,
                    'type': 'socket',
                    'family': self._get_family_name(conn.family),
                    'type_name': self._get_type_name(conn.type),
                    'local': f"{conn.laddr.ip}:{conn.laddr.port}" if conn.laddr else "N/A",
                    'remote': f"{conn.raddr.ip}:{conn.raddr.port}" if conn.raddr else "N/A",
                    'status': conn.status
                })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        
        return fds
    
    def _get_family_name(self, family):
        """Convert socket family constant to name."""
        families = {
            socket.AF_INET: 'IPv4',
            socket.AF_INET6: 'IPv6',
            socket.AF_UNIX: 'Unix',
        }
        return families.get(family, f'Unknown({family})')
    
    def _get_type_name(self, sock_type):
        """Convert socket type constant to name."""
        types = {
            socket.SOCK_STREAM: 'TCP',
            socket.SOCK_DGRAM: 'UDP',
            socket.SOCK_RAW: 'RAW',
        }
        return types.get(sock_type, f'Unknown({sock_type})')
    
    def start_monitoring(self, interval=0.5):
        """Start monitoring file descriptors in a separate thread."""
        self.monitoring = True
        monitor_thread = threading.Thread(target=self._monitor_loop, args=(interval,))
        monitor_thread.daemon = True
        monitor_thread.start()
        return monitor_thread
    
    def stop_monitoring(self):
        """Stop the monitoring thread."""
        self.monitoring = False
    
    def _monitor_loop(self, interval):
        """Background monitoring loop."""
        while self.monitoring:
            fds = self.get_open_fds()
            timestamp = datetime.now()
            
            for fd in fds:
                self.fd_history[fd['fd']].append({
                    'timestamp': timestamp,
                    'info': fd
                })
            
            time.sleep(interval)
    
    def print_current_fds(self):
        """Print current file descriptors."""
        fds = self.get_open_fds()
        
        print(f"\n{'='*80}")
        print(f"File Descriptors for PID {self.pid} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
        print(f"{'='*80}")
        
        # Separate by type
        files = [fd for fd in fds if fd['type'] == 'file']
        sockets = [fd for fd in fds if fd['type'] == 'socket']
        
        if files:
            print("\nFiles:")
            print(f"{'FD':>4} {'Path':<50}")
            print("-" * 55)
            for fd in sorted(files, key=lambda x: x['fd']):
                print(f"{fd['fd']:>4} {fd['path']:<50}")
        
        if sockets:
            print("\nSockets:")
            print(f"{'FD':>4} {'Type':<6} {'Family':<6} {'Local':<22} {'Remote':<22} {'Status':<12}")
            print("-" * 80)
            for fd in sorted(sockets, key=lambda x: x['fd']):
                print(f"{fd['fd']:>4} {fd['type_name']:<6} {fd['family']:<6} "
                      f"{fd['local']:<22} {fd['remote']:<22} {fd['status']:<12}")
        
        print(f"\nTotal: {len(files)} files, {len(sockets)} sockets")

def demonstrate_fd_lifecycle():
    """Demonstrate file descriptor lifecycle with monitoring."""
    monitor = FileDescriptorMonitor()
    
    print("Starting File Descriptor Monitoring Demo")
    print("This will show how file descriptors change during network operations\n")
    
    # Start monitoring
    monitor_thread = monitor.start_monitoring(interval=0.1)
    
    # Initial state
    print("1. Initial state - base file descriptors:")
    monitor.print_current_fds()
    time.sleep(1)
    
    # Create some sockets
    print("\n2. Creating 3 TCP sockets...")
    sockets = []
    for i in range(3):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sockets.append(s)
        print(f"   Created socket {i+1} with fd={s.fileno()}")
    
    time.sleep(0.5)
    monitor.print_current_fds()
    time.sleep(1)
    
    # Create a server and accept connections
    print("\n3. Creating a server socket and accepting connections...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('127.0.0.1', 0))
    server.listen(5)
    server_addr, server_port = server.getsockname()
    print(f"   Server listening on {server_addr}:{server_port} (fd={server.fileno()})")
    
    # Create client connections
    clients = []
    for i in range(2):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((server_addr, server_port))
        clients.append(client)
        conn, addr = server.accept()
        print(f"   Client {i+1} connected: fd={client.fileno()} -> server conn fd={conn.fileno()}")
        conn.close()
    
    time.sleep(0.5)
    monitor.print_current_fds()
    time.sleep(1)
    
    # Clean up some sockets
    print("\n4. Closing half of the sockets...")
    for i in range(len(sockets) // 2):
        fd = sockets[i].fileno()
        sockets[i].close()
        print(f"   Closed socket with fd={fd}")
    
    time.sleep(0.5)
    monitor.print_current_fds()
    
    # Stop monitoring
    monitor.stop_monitoring()
    monitor_thread.join()
    
    # Cleanup
    for s in sockets:
        try:
            s.close()
        except:
            pass
    for c in clients:
        c.close()
    server.close()
    
    print("\n5. All sockets closed - back to initial state")
    monitor.print_current_fds()

def track_fd_limits():
    """Show file descriptor limits and usage."""
    print("\n" + "="*60)
    print("File Descriptor Limits and Usage")
    print("="*60)
    
    # Get soft and hard limits
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        print(f"\nFile descriptor limits:")
        print(f"  Soft limit: {soft}")
        print(f"  Hard limit: {hard}")
    except:
        print("\nCould not determine file descriptor limits")
    
    # Current usage
    monitor = FileDescriptorMonitor()
    fds = monitor.get_open_fds()
    print(f"\nCurrent usage:")
    print(f"  Open file descriptors: {len(fds)}")
    
    # System-wide information (Linux specific)
    if sys.platform.startswith('linux'):
        try:
            with open('/proc/sys/fs/file-nr', 'r') as f:
                allocated, unused, max_fds = map(int, f.read().strip().split())
                print(f"\nSystem-wide file descriptors:")
                print(f"  Allocated: {allocated}")
                print(f"  Maximum: {max_fds}")
                print(f"  Usage: {allocated/max_fds*100:.1f}%")
        except:
            pass

def main():
    """Run the file descriptor monitoring demonstration."""
    try:
        # Check if we have required permissions
        test_proc = psutil.Process()
        test_proc.open_files()
        
        demonstrate_fd_lifecycle()
        track_fd_limits()
        
    except psutil.AccessDenied:
        print("Error: This script requires permissions to access process information.")
        print("Try running with appropriate privileges.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
