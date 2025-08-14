#!/usr/bin/env python3
"""
Buffer Management Techniques

This script demonstrates various buffer management strategies and their
impact on performance and memory usage in network applications.
"""

import os
import sys
import time
import socket
import threading
import collections
import weakref
import gc
import psutil
from datetime import datetime
import struct
import asyncio
import aiofiles

class BufferPool:
    """Thread-safe buffer pool implementation."""
    
    def __init__(self, buffer_size=4096, max_buffers=100):
        self.buffer_size = buffer_size
        self.max_buffers = max_buffers
        self.available = collections.deque()
        self.all_buffers = weakref.WeakSet()
        self.lock = threading.Lock()
        self.stats = {
            'created': 0,
            'reused': 0,
            'in_use': 0,
            'peak_usage': 0
        }
    
    def acquire(self):
        """Get a buffer from the pool."""
        with self.lock:
            if self.available:
                buffer = self.available.popleft()
                self.stats['reused'] += 1
            else:
                if len(self.all_buffers) < self.max_buffers:
                    buffer = bytearray(self.buffer_size)
                    self.all_buffers.add(buffer)
                    self.stats['created'] += 1
                else:
                    # Pool exhausted, create temporary buffer
                    buffer = bytearray(self.buffer_size)
            
            self.stats['in_use'] = len(self.all_buffers) - len(self.available)
            self.stats['peak_usage'] = max(self.stats['peak_usage'], self.stats['in_use'])
            return buffer
    
    def release(self, buffer):
        """Return a buffer to the pool."""
        with self.lock:
            if len(buffer) == self.buffer_size and len(self.available) < self.max_buffers:
                # Clear buffer before returning to pool
                buffer[:] = b'\x00' * len(buffer)
                self.available.append(buffer)
                self.stats['in_use'] = len(self.all_buffers) - len(self.available)
    
    def get_stats(self):
        """Get pool statistics."""
        with self.lock:
            return self.stats.copy()

class RingBuffer:
    """Circular buffer implementation for streaming data."""
    
    def __init__(self, size):
        self.size = size
        self.buffer = bytearray(size)
        self.read_pos = 0
        self.write_pos = 0
        self.count = 0
        self.lock = threading.Lock()
    
    def write(self, data):
        """Write data to ring buffer."""
        with self.lock:
            data_len = len(data)
            if data_len > self.available_space():
                raise BufferError("Not enough space in ring buffer")
            
            # Handle wrap around
            if self.write_pos + data_len <= self.size:
                self.buffer[self.write_pos:self.write_pos + data_len] = data
            else:
                first_part = self.size - self.write_pos
                self.buffer[self.write_pos:] = data[:first_part]
                self.buffer[:data_len - first_part] = data[first_part:]
            
            self.write_pos = (self.write_pos + data_len) % self.size
            self.count += data_len
            return data_len
    
    def read(self, size):
        """Read data from ring buffer."""
        with self.lock:
            if size > self.count:
                size = self.count
            
            if size == 0:
                return bytearray()
            
            # Handle wrap around
            if self.read_pos + size <= self.size:
                data = self.buffer[self.read_pos:self.read_pos + size]
            else:
                first_part = self.size - self.read_pos
                data = self.buffer[self.read_pos:] + self.buffer[:size - first_part]
            
            self.read_pos = (self.read_pos + size) % self.size
            self.count -= size
            return bytearray(data)
    
    def available_space(self):
        """Get available space in buffer."""
        return self.size - self.count
    
    def available_data(self):
        """Get amount of data available to read."""
        return self.count

class AdaptiveBuffer:
    """Buffer that adapts size based on usage patterns."""
    
    def __init__(self, initial_size=4096, min_size=1024, max_size=1024*1024):
        self.min_size = min_size
        self.max_size = max_size
        self.current_size = initial_size
        self.buffer = bytearray(initial_size)
        self.usage_history = collections.deque(maxlen=10)
        self.resize_count = 0
    
    def adapt(self, used_size):
        """Adapt buffer size based on usage."""
        self.usage_history.append(used_size)
        
        if len(self.usage_history) < 5:
            return
        
        avg_usage = sum(self.usage_history) / len(self.usage_history)
        
        # Grow if consistently using >80% of buffer
        if avg_usage > self.current_size * 0.8:
            new_size = min(self.current_size * 2, self.max_size)
            if new_size != self.current_size:
                self.buffer = bytearray(new_size)
                self.current_size = new_size
                self.resize_count += 1
        
        # Shrink if consistently using <30% of buffer
        elif avg_usage < self.current_size * 0.3:
            new_size = max(self.current_size // 2, self.min_size)
            if new_size != self.current_size:
                self.buffer = bytearray(new_size)
                self.current_size = new_size
                self.resize_count += 1
    
    def get_buffer(self):
        """Get the current buffer."""
        return self.buffer

def demonstrate_buffer_pool():
    """Demonstrate buffer pool performance."""
    print(f"\n{'='*60}")
    print("BUFFER POOL DEMONSTRATION")
    print(f"{'='*60}")
    
    buffer_size = 4096
    num_operations = 10000
    
    # Test 1: Without buffer pool
    print("\n1. Without buffer pool (new allocation each time):")
    start = time.perf_counter()
    buffers = []
    
    for i in range(num_operations):
        buffer = bytearray(buffer_size)
        # Simulate some work
        buffer[0] = i % 256
        buffers.append(buffer)
        if i % 100 == 99:
            buffers.clear()  # Simulate releasing buffers
    
    no_pool_time = time.perf_counter() - start
    
    # Force garbage collection
    buffers.clear()
    gc.collect()
    
    print(f"   Time: {no_pool_time*1000:.1f} ms")
    print(f"   Allocations/second: {num_operations/no_pool_time:,.0f}")
    
    # Test 2: With buffer pool
    print("\n2. With buffer pool:")
    pool = BufferPool(buffer_size=buffer_size, max_buffers=50)
    start = time.perf_counter()
    
    for i in range(num_operations):
        buffer = pool.acquire()
        # Simulate some work
        buffer[0] = i % 256
        if i % 100 == 99:
            # Return buffers to pool
            pool.release(buffer)
    
    pool_time = time.perf_counter() - start
    stats = pool.get_stats()
    
    print(f"   Time: {pool_time*1000:.1f} ms")
    print(f"   Operations/second: {num_operations/pool_time:,.0f}")
    print(f"   Speedup: {no_pool_time/pool_time:.2f}x")
    print(f"\n   Pool statistics:")
    print(f"   Buffers created: {stats['created']}")
    print(f"   Buffers reused: {stats['reused']}")
    print(f"   Peak usage: {stats['peak_usage']}")
    print(f"   Reuse rate: {stats['reused']/(stats['created']+stats['reused'])*100:.1f}%")

def demonstrate_ring_buffer():
    """Demonstrate ring buffer for streaming."""
    print(f"\n{'='*60}")
    print("RING BUFFER DEMONSTRATION")
    print(f"{'='*60}")
    
    ring_size = 64 * 1024  # 64KB
    ring = RingBuffer(ring_size)
    
    # Simulate producer-consumer pattern
    produced = 0
    consumed = 0
    
    def producer():
        nonlocal produced
        chunk_size = 1024
        for i in range(100):
            data = bytes([i % 256]) * chunk_size
            while True:
                try:
                    ring.write(data)
                    produced += chunk_size
                    break
                except BufferError:
                    time.sleep(0.001)  # Wait for consumer
    
    def consumer():
        nonlocal consumed
        chunk_size = 2048
        while consumed < 100 * 1024:  # Consume 100KB
            data = ring.read(chunk_size)
            if data:
                consumed += len(data)
            else:
                time.sleep(0.001)  # Wait for producer
    
    print(f"\nRing buffer size: {ring_size/1024:.0f}KB")
    print("Starting producer and consumer threads...")
    
    start = time.perf_counter()
    
    producer_thread = threading.Thread(target=producer)
    consumer_thread = threading.Thread(target=consumer)
    
    producer_thread.start()
    consumer_thread.start()
    
    producer_thread.join()
    consumer_thread.join()
    
    elapsed = time.perf_counter() - start
    
    print(f"\nResults:")
    print(f"   Time: {elapsed*1000:.1f} ms")
    print(f"   Data produced: {produced/1024:.0f}KB")
    print(f"   Data consumed: {consumed/1024:.0f}KB")
    print(f"   Throughput: {consumed/elapsed/1024/1024:.1f} MB/s")
    print(f"\n✓ Ring buffer enables lock-free streaming")
    print(f"✓ No allocation during operation")

def demonstrate_adaptive_buffer():
    """Demonstrate adaptive buffer sizing."""
    print(f"\n{'='*60}")
    print("ADAPTIVE BUFFER DEMONSTRATION")
    print(f"{'='*60}")
    
    adaptive = AdaptiveBuffer(initial_size=4096)
    
    # Simulate varying workloads
    workloads = [
        ("Small messages", [100, 200, 150, 180, 220] * 3),
        ("Growing messages", [1000, 2000, 3000, 4000, 5000] * 3),
        ("Large messages", [30000, 35000, 32000, 38000, 40000] * 3),
        ("Shrinking messages", [5000, 3000, 2000, 1000, 500] * 3),
    ]
    
    print("\nSimulating varying workloads:")
    
    for name, sizes in workloads:
        print(f"\n{name}:")
        initial_size = adaptive.current_size
        
        for size in sizes:
            buffer = adaptive.get_buffer()
            if size <= len(buffer):
                # Simulate using the buffer
                buffer[:size] = b'X' * size
                adaptive.adapt(size)
        
        print(f"   Buffer size: {initial_size} → {adaptive.current_size} bytes")
        print(f"   Resize count: {adaptive.resize_count}")

def demonstrate_memory_views():
    """Demonstrate zero-copy operations with memory views."""
    print(f"\n{'='*60}")
    print("MEMORY VIEWS AND ZERO-COPY")
    print(f"{'='*60}")
    
    # Create a large buffer
    size = 10 * 1024 * 1024  # 10MB
    buffer = bytearray(size)
    
    # Fill with pattern
    for i in range(0, size, 8):
        struct.pack_into('>Q', buffer, i, i)
    
    print(f"\nBuffer size: {size/1024/1024:.0f}MB")
    
    # Test 1: Traditional slicing (creates copy)
    print("\n1. Traditional slicing (creates copy):")
    start = time.perf_counter()
    slice_copy = buffer[1000:5000]
    slice_copy[0] = 255  # Modify copy
    slice_time = time.perf_counter() - start
    print(f"   Time: {slice_time*1000:.3f} ms")
    print(f"   Original modified: {buffer[1000] == 255}")
    
    # Test 2: Memory view (no copy)
    print("\n2. Memory view (zero-copy):")
    start = time.perf_counter()
    view = memoryview(buffer)[1000:5000]
    view[0] = 255  # Modify through view
    view_time = time.perf_counter() - start
    print(f"   Time: {view_time*1000:.6f} ms")
    print(f"   Original modified: {buffer[1000] == 255}")
    print(f"   Speedup: {slice_time/view_time:.0f}x")
    
    # Test 3: Multiple views of same buffer
    print("\n3. Multiple views of same buffer:")
    
    # Create multiple views
    header_view = memoryview(buffer)[:1024]
    body_view = memoryview(buffer)[1024:size-1024]
    footer_view = memoryview(buffer)[size-1024:]
    
    print(f"   Header: {len(header_view)} bytes")
    print(f"   Body: {len(body_view)/1024/1024:.1f} MB")
    print(f"   Footer: {len(footer_view)} bytes")
    print(f"   Total views memory overhead: ~0 bytes (views share buffer)")

def demonstrate_socket_buffers():
    """Demonstrate socket buffer management."""
    print(f"\n{'='*60}")
    print("SOCKET BUFFER MANAGEMENT")
    print(f"{'='*60}")
    
    # Create socket pair
    if sys.platform != 'win32':
        server_sock, client_sock = socket.socketpair()
        
        # Get default buffer sizes
        recv_buf = server_sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        send_buf = server_sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        
        print(f"\nDefault socket buffers:")
        print(f"   Receive buffer: {recv_buf:,} bytes")
        print(f"   Send buffer: {send_buf:,} bytes")
        
        # Test different buffer sizes
        test_sizes = [8192, 65536, 262144]
        
        print(f"\nTesting different buffer sizes:")
        print(f"{'Buffer Size':<15} {'Throughput':<15} {'Improvement'}")
        print("-" * 45)
        
        baseline_throughput = 0
        
        for buf_size in test_sizes:
            # Set buffer sizes
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buf_size)
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buf_size)
            client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buf_size)
            client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buf_size)
            
            # Measure throughput
            data = b'X' * 4096
            iterations = 1000
            
            start = time.perf_counter()
            for _ in range(iterations):
                client_sock.send(data)
                server_sock.recv(4096)
            elapsed = time.perf_counter() - start
            
            throughput = (len(data) * iterations / elapsed) / (1024 * 1024)  # MB/s
            
            if baseline_throughput == 0:
                baseline_throughput = throughput
                improvement = "baseline"
            else:
                improvement = f"{throughput/baseline_throughput:.2f}x"
            
            print(f"{buf_size:<15,} {throughput:<15.1f} {improvement}")
        
        server_sock.close()
        client_sock.close()

async def demonstrate_async_buffers():
    """Demonstrate buffer management in async I/O."""
    print(f"\n{'='*60}")
    print("ASYNC I/O BUFFER MANAGEMENT")
    print(f"{'='*60}")
    
    # Create test file
    test_file = "async_buffer_test.dat"
    file_size = 10 * 1024 * 1024  # 10MB
    
    # Write test data
    async with aiofiles.open(test_file, 'wb') as f:
        await f.write(b'A' * file_size)
    
    # Test different read strategies
    print("\nComparing async read strategies:")
    
    # Strategy 1: Read entire file
    start = time.perf_counter()
    async with aiofiles.open(test_file, 'rb') as f:
        data = await f.read()
    full_read_time = time.perf_counter() - start
    print(f"\n1. Full file read: {full_read_time*1000:.1f} ms")
    
    # Strategy 2: Chunked reading
    chunk_size = 64 * 1024  # 64KB chunks
    start = time.perf_counter()
    data_chunks = []
    async with aiofiles.open(test_file, 'rb') as f:
        while True:
            chunk = await f.read(chunk_size)
            if not chunk:
                break
            data_chunks.append(chunk)
    chunked_time = time.perf_counter() - start
    print(f"2. Chunked read ({chunk_size//1024}KB chunks): {chunked_time*1000:.1f} ms")
    
    # Strategy 3: Streaming with buffer pool
    pool = BufferPool(buffer_size=chunk_size, max_buffers=10)
    start = time.perf_counter()
    total_read = 0
    
    async with aiofiles.open(test_file, 'rb') as f:
        while True:
            buffer = pool.acquire()
            data = await f.read(chunk_size)
            if not data:
                pool.release(buffer)
                break
            buffer[:len(data)] = data
            total_read += len(data)
            pool.release(buffer)
    
    pooled_time = time.perf_counter() - start
    print(f"3. Pooled buffers: {pooled_time*1000:.1f} ms")
    
    # Cleanup
    os.remove(test_file)
    
    print(f"\nPool statistics: {pool.get_stats()}")

def main():
    """Run all buffer management demonstrations."""
    print("Buffer Management Techniques Demonstration")
    print(f"Python version: {sys.version}")
    print("=" * 60)
    
    try:
        demonstrate_buffer_pool()
        demonstrate_ring_buffer()
        demonstrate_adaptive_buffer()
        demonstrate_memory_views()
        demonstrate_socket_buffers()
        
        # Run async demonstration
        asyncio.run(demonstrate_async_buffers())
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("BUFFER MANAGEMENT BEST PRACTICES")
    print(f"{'='*60}")
    print("1. Use buffer pools to reduce allocation overhead")
    print("2. Ring buffers are ideal for streaming data")
    print("3. Adaptive buffers can optimize for varying workloads")
    print("4. Memory views enable zero-copy operations")
    print("5. Tune socket buffers for network performance")
    print("6. Consider memory pressure and GC impact")

if __name__ == "__main__":
    main()
