#!/usr/bin/env python3
"""
Socket Basics: Understanding File Descriptors and Socket Creation

This script demonstrates the fundamental concepts of socket programming,
showing how sockets are created and managed at the system level.
"""

import socket
import sys
import os
import time
from datetime import datetime

def print_header(title):
    """Print a formatted header."""
    print(f"\n{'='*60}")
    print(f"{title:^60}")
    print(f"{'='*60}\n")

def explore_file_descriptors():
    """Demonstrate file descriptors for standard I/O and sockets."""
    print_header("File Descriptors Exploration")
    
    # Standard file descriptors
    print("Standard file descriptors:")
    print(f"  stdin  (fd={sys.stdin.fileno()}): {sys.stdin.name}")
    print(f"  stdout (fd={sys.stdout.fileno()}): {sys.stdout.name}")
    print(f"  stderr (fd={sys.stderr.fileno()}): {sys.stderr.name}")
    
    # Create different types of sockets
    print("\nCreating sockets and their file descriptors:")
    
    # TCP socket
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"  TCP socket: fd={tcp_sock.fileno()}")
    
    # UDP socket
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(f"  UDP socket: fd={udp_sock.fileno()}")
    
    # Unix domain socket
    unix_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    print(f"  Unix socket: fd={unix_sock.fileno()}")
    
    # Get socket details
    print("\nSocket details:")
    for sock, name in [(tcp_sock, "TCP"), (udp_sock, "UDP")]:
        print(f"\n  {name} Socket:")
        print(f"    Type: {sock.type}")
        print(f"    Family: {sock.family}")
        print(f"    Timeout: {sock.gettimeout()}")
        
    # Cleanup
    tcp_sock.close()
    udp_sock.close()
    unix_sock.close()

def demonstrate_socket_options():
    """Show various socket options and their effects."""
    print_header("Socket Options")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Get and display socket options
    options = [
        (socket.SOL_SOCKET, socket.SO_REUSEADDR, "SO_REUSEADDR"),
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, "SO_KEEPALIVE"),
        (socket.SOL_SOCKET, socket.SO_RCVBUF, "SO_RCVBUF"),
        (socket.SOL_SOCKET, socket.SO_SNDBUF, "SO_SNDBUF"),
    ]
    
    print("Current socket options:")
    for level, optname, name in options:
        try:
            value = sock.getsockopt(level, optname)
            print(f"  {name}: {value}")
        except:
            print(f"  {name}: Not available")
    
    # Set socket options
    print("\nSetting socket options:")
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print("  ✓ Enabled SO_REUSEADDR (allows immediate port reuse)")
    
    # Modify buffer sizes
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
    print("  ✓ Set receive and send buffers to 64KB")
    
    sock.close()

def socket_state_lifecycle():
    """Demonstrate the socket state lifecycle."""
    print_header("Socket State Lifecycle")
    
    # Create a server socket to show the full lifecycle
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    print("1. Socket created (state: CLOSED)")
    print(f"   File descriptor: {server.fileno()}")
    
    # Bind
    server.bind(('127.0.0.1', 0))  # 0 means any available port
    addr, port = server.getsockname()
    print(f"\n2. Socket bound to {addr}:{port} (state: BOUND)")
    
    # Listen
    server.listen(5)
    print(f"\n3. Socket listening with backlog=5 (state: LISTENING)")
    
    # Create a client to connect
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"\n4. Client socket created (fd={client.fileno()})")
    
    # Non-blocking mode for demonstration
    server.setblocking(False)
    
    # Connect client
    try:
        client.connect((addr, port))
        print(f"\n5. Client connecting to {addr}:{port}")
    except BlockingIOError:
        pass  # Expected in non-blocking mode
    
    # Accept connection
    try:
        conn, client_addr = server.accept()
        print(f"\n6. Server accepted connection from {client_addr}")
        print(f"   New socket fd: {conn.fileno()}")
        conn.close()
    except BlockingIOError:
        print("\n6. No connection ready (non-blocking mode)")
    
    # Cleanup
    client.close()
    server.close()
    print("\n7. All sockets closed")

def measure_syscall_overhead():
    """Measure the overhead of socket system calls."""
    print_header("System Call Overhead Measurement")
    
    iterations = 10000
    
    # Measure socket creation
    start = time.perf_counter()
    for _ in range(iterations):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.close()
    end = time.perf_counter()
    
    create_time = (end - start) / iterations * 1_000_000  # Convert to microseconds
    print(f"Socket create/close overhead: {create_time:.2f} μs per operation")
    
    # Measure getsockopt
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    start = time.perf_counter()
    for _ in range(iterations):
        s.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
    end = time.perf_counter()
    s.close()
    
    getopt_time = (end - start) / iterations * 1_000_000
    print(f"getsockopt overhead: {getopt_time:.2f} μs per operation")
    
    # Compare with simple function call
    def dummy_function():
        return 42
    
    start = time.perf_counter()
    for _ in range(iterations):
        dummy_function()
    end = time.perf_counter()
    
    func_time = (end - start) / iterations * 1_000_000
    print(f"Python function call: {func_time:.2f} μs per operation")
    print(f"\nSystem call overhead ratio: {create_time/func_time:.1f}x slower than function call")

def main():
    """Run all demonstrations."""
    print(f"Socket Programming Fundamentals")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Python version: {sys.version.split()[0]}")
    print(f"Platform: {sys.platform}")
    
    try:
        explore_file_descriptors()
        demonstrate_socket_options()
        socket_state_lifecycle()
        measure_syscall_overhead()
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60)
    print("Demonstration complete!")

if __name__ == "__main__":
    main()
