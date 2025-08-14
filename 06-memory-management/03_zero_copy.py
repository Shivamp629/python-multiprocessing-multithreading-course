#!/usr/bin/env python3
"""
Zero-Copy Techniques and Performance

This script demonstrates zero-copy techniques and their performance benefits
compared to traditional copying methods.
"""

import os
import sys
import time
import socket
import mmap
import struct
import threading
import multiprocessing as mp
from multiprocessing import shared_memory
import numpy as np
import psutil

class ZeroCopyDemo:
    """Demonstrates various zero-copy techniques."""
    
    def __init__(self):
        self.process = psutil.Process()
    
    def demonstrate_sendfile(self):
        """Demonstrate sendfile() for zero-copy file transfer."""
        print(f"\n{'='*60}")
        print("SENDFILE() ZERO-COPY DEMONSTRATION")
        print(f"{'='*60}")
        
        if not hasattr(os, 'sendfile'):
            print("sendfile() not available on this platform")
            return
        
        # Create test file
        file_size = 50 * 1024 * 1024  # 50MB
        test_file = "sendfile_test.dat"
        
        print(f"\nCreating {file_size/1024/1024:.0f}MB test file...")
        with open(test_file, 'wb') as f:
            # Write in chunks to avoid memory spike
            chunk = b'A' * (1024 * 1024)
            for _ in range(file_size // len(chunk)):
                f.write(chunk)
        
        # Create socket pair
        server_sock, client_sock = socket.socketpair()
        
        # Method 1: Traditional read/write
        print("\n1. Traditional read/write method:")
        start = time.perf_counter()
        
        with open(test_file, 'rb') as f:
            bytes_sent = 0
            while True:
                chunk = f.read(65536)  # 64KB chunks
                if not chunk:
                    break
                client_sock.sendall(chunk)
                bytes_sent += len(chunk)
        
        # Drain the socket
        total_received = 0
        while total_received < file_size:
            data = server_sock.recv(65536)
            total_received += len(data)
        
        traditional_time = time.perf_counter() - start
        print(f"   Time: {traditional_time:.2f}s")
        print(f"   Throughput: {file_size/traditional_time/1024/1024:.1f} MB/s")
        
        # Method 2: sendfile() zero-copy
        print("\n2. sendfile() zero-copy method:")
        
        # Reset socket pair
        server_sock.close()
        client_sock.close()
        server_sock, client_sock = socket.socketpair()
        
        start = time.perf_counter()
        
        with open(test_file, 'rb') as f:
            # sendfile() directly transfers from file to socket
            bytes_sent = os.sendfile(client_sock.fileno(), f.fileno(), 0, file_size)
        
        # Drain the socket
        total_received = 0
        while total_received < file_size:
            data = server_sock.recv(65536)
            total_received += len(data)
        
        sendfile_time = time.perf_counter() - start
        print(f"   Time: {sendfile_time:.2f}s")
        print(f"   Throughput: {file_size/sendfile_time/1024/1024:.1f} MB/s")
        print(f"   Speedup: {traditional_time/sendfile_time:.2f}x")
        
        # Cleanup
        server_sock.close()
        client_sock.close()
        os.remove(test_file)
        
        print("\n✓ sendfile() avoids copying data to user space")
        print("✓ Data goes directly from page cache to socket")
    
    def demonstrate_mmap_zerocopy(self):
        """Demonstrate zero-copy with memory mapping."""
        print(f"\n{'='*60}")
        print("MEMORY-MAPPED I/O ZERO-COPY")
        print(f"{'='*60}")
        
        file_size = 100 * 1024 * 1024  # 100MB
        test_file = "mmap_test.dat"
        
        # Create test file with pattern
        print(f"\nCreating {file_size/1024/1024:.0f}MB test file...")
        with open(test_file, 'wb') as f:
            for i in range(0, file_size, 8):
                f.write(struct.pack('>Q', i))
        
        # Method 1: Traditional file processing
        print("\n1. Traditional file processing:")
        start = time.perf_counter()
        
        with open(test_file, 'rb') as f:
            data = f.read()
            # Process data: count specific pattern
            count = data.count(b'\x00\x00\x00\x00')
        
        traditional_time = time.perf_counter() - start
        print(f"   Time: {traditional_time:.2f}s")
        print(f"   Pattern count: {count}")
        
        # Method 2: Memory-mapped processing
        print("\n2. Memory-mapped processing (zero-copy):")
        start = time.perf_counter()
        
        with open(test_file, 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                # Process directly in mapped memory
                count = mm[:].count(b'\x00\x00\x00\x00')
        
        mmap_time = time.perf_counter() - start
        print(f"   Time: {mmap_time:.2f}s")
        print(f"   Pattern count: {count}")
        print(f"   Speedup: {traditional_time/mmap_time:.2f}x")
        
        # Method 3: Memory-mapped with views
        print("\n3. Memory-mapped with memory views:")
        start = time.perf_counter()
        
        with open(test_file, 'r+b') as f:
            with mmap.mmap(f.fileno(), 0) as mm:
                # Create numpy array view (no copy)
                array = np.frombuffer(mm, dtype=np.uint64)
                # Fast numpy operations
                zeros = np.count_nonzero(array == 0)
        
        numpy_time = time.perf_counter() - start
        print(f"   Time: {numpy_time:.2f}s")
        print(f"   Zero values: {zeros}")
        print(f"   Speedup vs traditional: {traditional_time/numpy_time:.2f}x")
        
        # Cleanup
        os.remove(test_file)
        
        print("\n✓ Memory mapping avoids copying file data")
        print("✓ NumPy can work directly on mapped memory")
    
    def demonstrate_shared_memory_zerocopy(self):
        """Demonstrate zero-copy IPC with shared memory."""
        print(f"\n{'='*60}")
        print("SHARED MEMORY ZERO-COPY IPC")
        print(f"{'='*60}")
        
        if sys.version_info < (3, 8):
            print("Shared memory requires Python 3.8+")
            return
        
        # Data size
        array_size = 10 * 1024 * 1024  # 10M elements
        dtype = np.float64
        
        def traditional_ipc_worker(queue, array_size):
            """Worker using traditional queue IPC."""
            # Create data
            data = np.random.rand(array_size)
            # Send through queue (requires pickling)
            queue.put(data)
        
        def shared_memory_worker(shm_name, shape, dtype):
            """Worker using shared memory."""
            # Attach to existing shared memory
            existing_shm = shared_memory.SharedMemory(name=shm_name)
            # Create array view (no copy)
            array = np.ndarray(shape, dtype=dtype, buffer=existing_shm.buf)
            # Modify in place
            array[:] = np.random.rand(*shape)
            # Clean up
            existing_shm.close()
        
        # Method 1: Traditional queue-based IPC
        print("\n1. Traditional queue-based IPC:")
        queue = mp.Queue()
        
        start = time.perf_counter()
        
        # Start worker
        p = mp.Process(target=traditional_ipc_worker, args=(queue, array_size))
        p.start()
        
        # Receive data (requires unpickling)
        received_data = queue.get()
        p.join()
        
        traditional_time = time.perf_counter() - start
        print(f"   Time: {traditional_time:.2f}s")
        print(f"   Data size: {received_data.nbytes / 1024 / 1024:.1f} MB")
        
        # Method 2: Shared memory zero-copy IPC
        print("\n2. Shared memory zero-copy IPC:")
        
        # Create shared memory
        shm = shared_memory.SharedMemory(
            create=True, 
            size=array_size * np.dtype(dtype).itemsize
        )
        
        start = time.perf_counter()
        
        # Start worker
        p = mp.Process(
            target=shared_memory_worker,
            args=(shm.name, (array_size,), dtype)
        )
        p.start()
        p.join()
        
        # Access data (no copy)
        shared_array = np.ndarray((array_size,), dtype=dtype, buffer=shm.buf)
        result = shared_array.mean()  # Process in place
        
        shm_time = time.perf_counter() - start
        print(f"   Time: {shm_time:.2f}s")
        print(f"   Data mean: {result:.6f}")
        print(f"   Speedup: {traditional_time/shm_time:.2f}x")
        
        # Cleanup
        shm.close()
        shm.unlink()
        
        print("\n✓ Shared memory avoids serialization")
        print("✓ Multiple processes see same physical memory")
    
    def demonstrate_buffer_protocol(self):
        """Demonstrate Python's buffer protocol for zero-copy."""
        print(f"\n{'='*60}")
        print("PYTHON BUFFER PROTOCOL")
        print(f"{'='*60}")
        
        # Create various buffer-compatible objects
        size = 10 * 1024 * 1024  # 10MB
        
        print(f"\nCreating {size/1024/1024:.0f}MB buffers...")
        
        # Different buffer types
        byte_array = bytearray(size)
        memory_view = memoryview(byte_array)
        numpy_array = np.zeros(size // 8, dtype=np.uint64)
        
        # Fill with test pattern
        for i in range(0, size, 8):
            struct.pack_into('>Q', byte_array, i, i)
        
        # Test 1: Processing with copies
        print("\n1. Processing with copies:")
        start = time.perf_counter()
        
        # Create copies
        copy1 = bytes(byte_array)  # Copy to bytes
        copy2 = bytearray(copy1)   # Copy to new bytearray
        # Process copy
        result = sum(copy2[::8])
        
        copy_time = time.perf_counter() - start
        print(f"   Time: {copy_time*1000:.1f} ms")
        print(f"   Result: {result}")
        
        # Test 2: Processing with views (zero-copy)
        print("\n2. Processing with views (zero-copy):")
        start = time.perf_counter()
        
        # Create views (no copy)
        view1 = memory_view[::8]  # Strided view
        view2 = numpy_array.view()  # NumPy view
        # Process through view
        result = sum(view1)
        
        view_time = time.perf_counter() - start
        print(f"   Time: {view_time*1000:.1f} ms")
        print(f"   Result: {result}")
        print(f"   Speedup: {copy_time/view_time:.2f}x")
        
        # Test 3: Demonstrate view modifications
        print("\n3. View modifications affect original:")
        original = bytearray(b'Hello World')
        view = memoryview(original)
        
        print(f"   Original: {original}")
        view[0] = ord('h')  # Modify through view
        print(f"   After view modification: {original}")
        print("   ✓ Views share memory with original")
    
    def demonstrate_splice(self):
        """Demonstrate splice() for zero-copy pipe operations."""
        print(f"\n{'='*60}")
        print("SPLICE() ZERO-COPY (Linux)")
        print(f"{'='*60}")
        
        if not sys.platform.startswith('linux'):
            print("splice() is Linux-specific")
            return
        
        try:
            import ctypes
            import ctypes.util
            
            # Load libc
            libc = ctypes.CDLL(ctypes.util.find_library('c'))
            
            # splice() signature
            # ssize_t splice(int fd_in, off_t *off_in, int fd_out, 
            #                off_t *off_out, size_t len, unsigned int flags)
            splice = libc.splice
            splice.argtypes = [
                ctypes.c_int, ctypes.POINTER(ctypes.c_longlong),
                ctypes.c_int, ctypes.POINTER(ctypes.c_longlong),
                ctypes.c_size_t, ctypes.c_uint
            ]
            splice.restype = ctypes.c_ssize_t
            
            SPLICE_F_MOVE = 1
            SPLICE_F_MORE = 4
            
            # Create test file
            test_file = "splice_test.dat"
            file_size = 10 * 1024 * 1024  # 10MB
            
            with open(test_file, 'wb') as f:
                f.write(b'X' * file_size)
            
            # Create pipes
            pipe_in_r, pipe_in_w = os.pipe()
            pipe_out_r, pipe_out_w = os.pipe()
            
            print("\nDemonstrating splice() between file and pipe:")
            
            # Open file
            fd = os.open(test_file, os.O_RDONLY)
            
            # Splice from file to pipe (zero-copy)
            start = time.perf_counter()
            bytes_spliced = splice(
                fd, None,
                pipe_in_w, None,
                file_size,
                SPLICE_F_MOVE | SPLICE_F_MORE
            )
            splice_time = time.perf_counter() - start
            
            print(f"   Spliced {bytes_spliced/1024/1024:.1f} MB")
            print(f"   Time: {splice_time*1000:.1f} ms")
            print(f"   Throughput: {bytes_spliced/splice_time/1024/1024:.1f} MB/s")
            
            # Cleanup
            os.close(fd)
            os.close(pipe_in_r)
            os.close(pipe_in_w)
            os.close(pipe_out_r)
            os.close(pipe_out_w)
            os.remove(test_file)
            
            print("\n✓ splice() moves data between file descriptors")
            print("✓ No user-space copying involved")
            
        except Exception as e:
            print(f"Could not demonstrate splice(): {e}")

def demonstrate_cow_fork():
    """Demonstrate Copy-on-Write (COW) with fork."""
    print(f"\n{'='*60}")
    print("COPY-ON-WRITE (COW) WITH FORK")
    print(f"{'='*60}")
    
    if not hasattr(os, 'fork'):
        print("fork() not available on this platform")
        return
    
    # Create large data structure
    size = 100 * 1024 * 1024  # 100MB
    print(f"\nCreating {size/1024/1024:.0f}MB data structure...")
    large_array = bytearray(size)
    
    # Fill with pattern
    for i in range(0, size, 1024):
        large_array[i:i+4] = struct.pack('>I', i)
    
    # Get memory usage before fork
    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024 / 1024
    
    print(f"Memory before fork: {mem_before:.1f} MB")
    
    # Fork process
    pid = os.fork()
    
    if pid == 0:  # Child process
        # Child can read parent's memory (COW)
        time.sleep(0.1)  # Let parent measure memory
        
        # Read data (no copy)
        total = sum(large_array[::1024])
        
        # Now modify data (triggers COW)
        for i in range(0, size, 1024*1024):
            large_array[i] = 255
        
        time.sleep(0.1)  # Let parent measure again
        os._exit(0)
        
    else:  # Parent process
        # Measure memory after fork (before child modifies)
        time.sleep(0.05)
        mem_after_fork = process.memory_info().rss / 1024 / 1024
        print(f"Memory after fork (COW): {mem_after_fork:.1f} MB")
        print(f"Increase: {mem_after_fork - mem_before:.1f} MB (minimal)")
        
        # Wait for child to modify data
        time.sleep(0.2)
        mem_after_modify = process.memory_info().rss / 1024 / 1024
        print(f"Memory after child modifies: {mem_after_modify:.1f} MB")
        
        # Wait for child
        os.waitpid(pid, 0)
        
        print("\n✓ COW delays copying until write")
        print("✓ Shared pages are copied only when modified")

def compare_all_methods():
    """Compare all zero-copy methods."""
    print(f"\n{'='*60}")
    print("ZERO-COPY METHODS COMPARISON")
    print(f"{'='*60}")
    
    print("\n" + "="*70)
    print(f"{'Method':<20} {'Use Case':<30} {'Performance':<20}")
    print("="*70)
    
    methods = [
        ("sendfile()", "File to socket transfer", "Excellent"),
        ("mmap()", "File processing", "Excellent"),
        ("splice()", "Pipe/socket operations", "Excellent"),
        ("Shared memory", "IPC between processes", "Excellent"),
        ("Buffer protocol", "In-process operations", "Very Good"),
        ("COW fork", "Process creation", "Good"),
        ("io_uring", "Async I/O (Linux 5.1+)", "Excellent"),
    ]
    
    for method, use_case, performance in methods:
        print(f"{method:<20} {use_case:<30} {performance:<20}")
    
    print("\n" + "="*70)
    print("\nPlatform support:")
    print("  sendfile(): Unix-like systems")
    print("  splice(): Linux only")
    print("  mmap(): Cross-platform")
    print("  Shared memory: Python 3.8+")
    print("  io_uring: Linux 5.1+")

def main():
    """Run all zero-copy demonstrations."""
    print("Zero-Copy Techniques and Performance")
    print(f"Python version: {sys.version}")
    print(f"Platform: {sys.platform}")
    print("=" * 60)
    
    demo = ZeroCopyDemo()
    
    try:
        demo.demonstrate_sendfile()
        demo.demonstrate_mmap_zerocopy()
        demo.demonstrate_shared_memory_zerocopy()
        demo.demonstrate_buffer_protocol()
        demo.demonstrate_splice()
        demonstrate_cow_fork()
        compare_all_methods()
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("ZERO-COPY BEST PRACTICES")
    print(f"{'='*60}")
    print("1. Use sendfile() for file-to-socket transfers")
    print("2. Memory map large files instead of reading")
    print("3. Share memory between processes when possible")
    print("4. Use buffer protocol and views in Python")
    print("5. Avoid unnecessary data copies in hot paths")
    print("6. Profile to identify copy bottlenecks")

if __name__ == "__main__":
    main()
