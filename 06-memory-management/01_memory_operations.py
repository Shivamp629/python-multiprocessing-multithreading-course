#!/usr/bin/env python3
"""
Memory Operations and System Calls

This script demonstrates the relationship between user space and kernel space,
showing how system calls work and their performance implications.
"""

import os
import sys
import time
import mmap
import psutil
import ctypes
import socket
import struct
import tracemalloc
from datetime import datetime
import multiprocessing as mp

class MemoryOperationsDemo:
    """Demonstrates memory operations and system calls."""
    
    def __init__(self):
        self.process = psutil.Process()
        self.page_size = os.sysconf('SC_PAGE_SIZE') if hasattr(os, 'sysconf') else 4096
        
    def demonstrate_system_calls(self):
        """Show system call overhead."""
        print(f"\n{'='*60}")
        print("SYSTEM CALL OVERHEAD DEMONSTRATION")
        print(f"{'='*60}")
        
        iterations = 100000
        
        # Test 1: getpid() - minimal system call
        print("\n1. Minimal system call (getpid):")
        start = time.perf_counter()
        for _ in range(iterations):
            os.getpid()
        elapsed = time.perf_counter() - start
        per_call = (elapsed / iterations) * 1_000_000  # Convert to microseconds
        
        print(f"   Total time: {elapsed:.3f}s for {iterations} calls")
        print(f"   Per call: {per_call:.3f} μs")
        print(f"   Calls/second: {iterations/elapsed:,.0f}")
        
        # Test 2: read() - I/O system call
        print("\n2. I/O system call (read from /dev/zero):")
        
        # Open /dev/zero for reading
        if sys.platform != 'win32':
            fd = os.open('/dev/zero', os.O_RDONLY)
            buffer = bytearray(1)
            
            start = time.perf_counter()
            for _ in range(iterations):
                os.read(fd, 1)
            elapsed = time.perf_counter() - start
            os.close(fd)
            
            per_call = (elapsed / iterations) * 1_000_000
            print(f"   Total time: {elapsed:.3f}s for {iterations} calls")
            print(f"   Per call: {per_call:.3f} μs")
            print(f"   Overhead vs getpid: {per_call/per_call:.1f}x")
        
        # Test 3: Memory allocation
        print("\n3. Memory allocation overhead:")
        
        # Small allocations
        start = time.perf_counter()
        allocations = []
        for _ in range(10000):
            allocations.append(bytearray(1024))  # 1KB each
        small_time = time.perf_counter() - start
        
        # Large allocation
        start = time.perf_counter()
        large_buffer = bytearray(10000 * 1024)  # 10MB at once
        large_time = time.perf_counter() - start
        
        print(f"   10,000 x 1KB allocations: {small_time*1000:.1f} ms")
        print(f"   1 x 10MB allocation: {large_time*1000:.3f} ms")
        print(f"   Efficiency gain: {small_time/large_time:.1f}x")
        
        # Show memory usage
        mem_info = self.process.memory_info()
        print(f"\n   Current memory usage:")
        print(f"   RSS (Resident): {mem_info.rss / 1024 / 1024:.1f} MB")
        print(f"   VMS (Virtual): {mem_info.vms / 1024 / 1024:.1f} MB")
    
    def demonstrate_memory_copying(self):
        """Demonstrate the cost of memory copying."""
        print(f"\n{'='*60}")
        print("MEMORY COPY OPERATIONS")
        print(f"{'='*60}")
        
        sizes = [1024, 10*1024, 100*1024, 1024*1024, 10*1024*1024]  # 1KB to 10MB
        
        print("\nCopy performance by size:")
        print(f"{'Size':<12} {'Time':<12} {'Bandwidth':<15} {'Method'}")
        print("-" * 50)
        
        for size in sizes:
            # Create source buffer
            src = bytearray(size)
            
            # Method 1: Python slice copy
            start = time.perf_counter()
            dst1 = src[:]
            py_time = time.perf_counter() - start
            bandwidth1 = (size / py_time) / (1024 * 1024)  # MB/s
            
            # Method 2: bytearray copy
            start = time.perf_counter()
            dst2 = bytearray(src)
            ba_time = time.perf_counter() - start
            bandwidth2 = (size / ba_time) / (1024 * 1024)
            
            # Method 3: memoryview (no copy)
            start = time.perf_counter()
            view = memoryview(src)
            mv_time = time.perf_counter() - start
            bandwidth3 = float('inf')  # No actual copy
            
            # Format size
            if size < 1024*1024:
                size_str = f"{size/1024:.0f} KB"
            else:
                size_str = f"{size/1024/1024:.0f} MB"
            
            print(f"{size_str:<12} {py_time*1000:<12.3f} {bandwidth1:<15.0f} Python slice")
            print(f"{'':12} {ba_time*1000:<12.3f} {bandwidth2:<15.0f} bytearray()")
            print(f"{'':12} {mv_time*1000:<12.6f} {'∞':<15} memoryview")
            print()
    
    def demonstrate_user_kernel_boundary(self):
        """Show data movement between user and kernel space."""
        print(f"\n{'='*60}")
        print("USER/KERNEL SPACE DATA TRANSFER")
        print(f"{'='*60}")
        
        # Create a socket pair for demonstration
        if sys.platform != 'win32':
            sock1, sock2 = socket.socketpair()
            
            # Test different buffer sizes
            buffer_sizes = [64, 512, 4096, 65536]
            
            print("\nSocket send/recv performance (user ↔ kernel):")
            print(f"{'Buffer Size':<15} {'Iterations':<12} {'Time (ms)':<12} {'MB/s':<10}")
            print("-" * 50)
            
            for buf_size in buffer_sizes:
                data = b'x' * buf_size
                iterations = 10000
                
                start = time.perf_counter()
                for _ in range(iterations):
                    sock1.send(data)
                    sock2.recv(buf_size)
                elapsed = time.perf_counter() - start
                
                total_bytes = buf_size * iterations
                bandwidth = (total_bytes / elapsed) / (1024 * 1024)
                
                print(f"{buf_size:<15} {iterations:<12} {elapsed*1000:<12.1f} {bandwidth:<10.1f}")
            
            sock1.close()
            sock2.close()
            
            print("\n✓ Each send() and recv() crosses user/kernel boundary")
            print("✓ Larger buffers amortize system call overhead")
    
    def demonstrate_memory_mapping(self):
        """Demonstrate memory mapping vs traditional I/O."""
        print(f"\n{'='*60}")
        print("MEMORY MAPPING VS TRADITIONAL I/O")
        print(f"{'='*60}")
        
        # Create a test file
        test_file = "test_mmap.dat"
        file_size = 10 * 1024 * 1024  # 10MB
        
        # Create file with data
        print(f"\nCreating {file_size/1024/1024:.0f}MB test file...")
        with open(test_file, 'wb') as f:
            f.write(b'A' * file_size)
        
        # Test 1: Traditional read
        print("\n1. Traditional file read:")
        start = time.perf_counter()
        with open(test_file, 'rb') as f:
            data = f.read()
            # Process data
            count = data.count(b'A')
        trad_time = time.perf_counter() - start
        print(f"   Time: {trad_time*1000:.1f} ms")
        print(f"   Found: {count} bytes")
        
        # Test 2: Memory mapped read
        print("\n2. Memory mapped read:")
        start = time.perf_counter()
        with open(test_file, 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                # Process data directly in memory
                count = mm[:].count(b'A')
        mmap_time = time.perf_counter() - start
        print(f"   Time: {mmap_time*1000:.1f} ms")
        print(f"   Found: {count} bytes")
        print(f"   Speedup: {trad_time/mmap_time:.2f}x")
        
        # Test 3: Memory mapped write
        print("\n3. Memory mapped write (modify file):")
        start = time.perf_counter()
        with open(test_file, 'r+b') as f:
            with mmap.mmap(f.fileno(), 0) as mm:
                # Modify first 1000 bytes
                mm[0:1000] = b'B' * 1000
                mm.flush()
        mmap_write_time = time.perf_counter() - start
        print(f"   Time: {mmap_write_time*1000:.3f} ms")
        
        # Cleanup
        os.remove(test_file)
        
        print("\n✓ Memory mapping avoids explicit read/write system calls")
        print("✓ Data is paged in/out as needed by the OS")
    
    def demonstrate_page_faults(self):
        """Demonstrate page faults and memory access patterns."""
        print(f"\n{'='*60}")
        print("PAGE FAULTS AND MEMORY ACCESS")
        print(f"{'='*60}")
        
        print(f"\nSystem page size: {self.page_size} bytes")
        
        # Allocate large memory region
        size = 100 * 1024 * 1024  # 100MB
        print(f"\nAllocating {size/1024/1024:.0f}MB of memory...")
        
        # Get initial page fault count
        initial_faults = self.process.memory_info().vms
        
        # Allocate but don't touch (virtual allocation)
        start = time.perf_counter()
        buffer = bytearray(size)
        alloc_time = time.perf_counter() - start
        
        print(f"   Allocation time: {alloc_time*1000:.3f} ms")
        
        # Touch every page (cause page faults)
        print("\nTouching all pages (forcing physical allocation)...")
        start = time.perf_counter()
        for i in range(0, size, self.page_size):
            buffer[i] = 1
        touch_time = time.perf_counter() - start
        
        num_pages = size // self.page_size
        print(f"   Touch time: {touch_time*1000:.1f} ms")
        print(f"   Pages touched: {num_pages}")
        print(f"   Time per page: {touch_time/num_pages*1000*1000:.1f} μs")
        
        # Access pattern comparison
        print("\nComparing memory access patterns:")
        
        # Sequential access
        start = time.perf_counter()
        total = 0
        for i in range(0, size, 64):  # Cache line size
            total += buffer[i]
        seq_time = time.perf_counter() - start
        
        # Random access
        import random
        indices = list(range(0, size, 64))
        random.shuffle(indices)
        
        start = time.perf_counter()
        total = 0
        for i in indices:
            total += buffer[i]
        rand_time = time.perf_counter() - start
        
        print(f"   Sequential access: {seq_time*1000:.1f} ms")
        print(f"   Random access: {rand_time*1000:.1f} ms")
        print(f"   Random penalty: {rand_time/seq_time:.1f}x slower")
        
        print("\n✓ Virtual allocation is cheap (no physical memory)")
        print("✓ Physical allocation happens on first access (page fault)")
        print("✓ Sequential access is cache-friendly")

def demonstrate_shared_memory():
    """Demonstrate shared memory between processes."""
    print(f"\n{'='*60}")
    print("SHARED MEMORY BETWEEN PROCESSES")
    print(f"{'='*60}")
    
    def worker_process(shared_array, index, iterations):
        """Worker that writes to shared memory."""
        for i in range(iterations):
            shared_array[index] = i
            time.sleep(0.0001)  # Small delay
    
    # Create shared memory
    size = 1024 * 1024  # 1MB
    shared_array = mp.Array('b', size)
    
    print(f"\nCreated {size/1024:.0f}KB of shared memory")
    
    # Start worker processes
    num_workers = 4
    iterations = 1000
    processes = []
    
    print(f"Starting {num_workers} worker processes...")
    start = time.perf_counter()
    
    for i in range(num_workers):
        p = mp.Process(target=worker_process, args=(shared_array, i*1000, iterations))
        p.start()
        processes.append(p)
    
    # Wait for completion
    for p in processes:
        p.join()
    
    elapsed = time.perf_counter() - start
    total_writes = num_workers * iterations
    
    print(f"\nResults:")
    print(f"   Total time: {elapsed:.2f}s")
    print(f"   Total writes: {total_writes}")
    print(f"   Writes/second: {total_writes/elapsed:,.0f}")
    print("\n✓ Shared memory avoids copying between processes")
    print("✓ All processes see the same physical memory")

def demonstrate_memory_bandwidth():
    """Measure memory bandwidth for different operations."""
    print(f"\n{'='*60}")
    print("MEMORY BANDWIDTH MEASUREMENT")
    print(f"{'='*60}")
    
    size = 100 * 1024 * 1024  # 100MB
    buffer = bytearray(size)
    
    # Fill buffer with data
    for i in range(0, size, 8):
        struct.pack_into('Q', buffer, i, i)
    
    iterations = 10
    
    # Test 1: Read bandwidth
    print("\n1. Sequential read bandwidth:")
    start = time.perf_counter()
    for _ in range(iterations):
        total = sum(buffer[::8])  # Read every 8 bytes
    elapsed = time.perf_counter() - start
    read_bandwidth = (size * iterations / elapsed) / (1024**3)  # GB/s
    print(f"   Bandwidth: {read_bandwidth:.2f} GB/s")
    
    # Test 2: Write bandwidth
    print("\n2. Sequential write bandwidth:")
    start = time.perf_counter()
    for _ in range(iterations):
        buffer[::8] = b'\xff' * (size // 8)
    elapsed = time.perf_counter() - start
    write_bandwidth = (size * iterations / elapsed) / (1024**3)
    print(f"   Bandwidth: {write_bandwidth:.2f} GB/s")
    
    # Test 3: Copy bandwidth
    print("\n3. Memory copy bandwidth:")
    dest = bytearray(size)
    start = time.perf_counter()
    for _ in range(iterations):
        dest[:] = buffer
    elapsed = time.perf_counter() - start
    copy_bandwidth = (size * iterations * 2 / elapsed) / (1024**3)  # *2 for read+write
    print(f"   Bandwidth: {copy_bandwidth:.2f} GB/s (combined read+write)")

def main():
    """Run all memory demonstrations."""
    print("Memory Operations and System Calls Demonstration")
    print(f"Python version: {sys.version}")
    print(f"Platform: {sys.platform}")
    print("=" * 60)
    
    demo = MemoryOperationsDemo()
    
    try:
        demo.demonstrate_system_calls()
        demo.demonstrate_memory_copying()
        demo.demonstrate_user_kernel_boundary()
        demo.demonstrate_memory_mapping()
        demo.demonstrate_page_faults()
        demonstrate_shared_memory()
        demonstrate_memory_bandwidth()
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("KEY TAKEAWAYS")
    print(f"{'='*60}")
    print("1. System calls have significant overhead (~100-300ns)")
    print("2. Batch operations to minimize user/kernel transitions")
    print("3. Memory mapping can avoid explicit read/write calls")
    print("4. Page faults occur on first access to virtual memory")
    print("5. Sequential access patterns are cache-friendly")
    print("6. Shared memory enables zero-copy IPC")

if __name__ == "__main__":
    main()
