#!/usr/bin/env python3
"""
Advanced Synchronization Primitives

This script demonstrates advanced synchronization patterns including
reader-writer locks, barriers, and custom synchronization primitives.
"""

import threading
import time
import random
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
import queue

class ReaderWriterLock:
    """A reader-writer lock implementation."""
    
    def __init__(self):
        self._readers = 0
        self._writers = 0
        self._read_ready = threading.Condition(threading.RLock())
        self._write_ready = threading.Condition(threading.RLock())
    
    @contextmanager
    def read_lock(self):
        """Acquire read lock."""
        self._read_ready.acquire()
        try:
            while self._writers > 0:
                self._read_ready.wait()
            self._readers += 1
        finally:
            self._read_ready.release()
        
        try:
            yield
        finally:
            self._read_ready.acquire()
            try:
                self._readers -= 1
                if self._readers == 0:
                    self._read_ready.notify_all()
            finally:
                self._read_ready.release()
    
    @contextmanager
    def write_lock(self):
        """Acquire write lock."""
        self._write_ready.acquire()
        try:
            while self._writers > 0 or self._readers > 0:
                self._write_ready.wait()
            self._writers += 1
        finally:
            self._write_ready.release()
        
        try:
            yield
        finally:
            self._write_ready.acquire()
            try:
                self._writers -= 1
                self._write_ready.notify_all()
            finally:
                self._write_ready.release()
            
            # Notify readers
            self._read_ready.acquire()
            self._read_ready.notify_all()
            self._read_ready.release()

class ThreadLocalContext:
    """Thread-local storage demonstration."""
    
    def __init__(self):
        self._local = threading.local()
        self._initializer = None
    
    def initialize(self, initializer):
        """Set initializer function for thread-local data."""
        self._initializer = initializer
    
    def get(self):
        """Get thread-local value."""
        if not hasattr(self._local, 'value'):
            if self._initializer:
                self._local.value = self._initializer()
            else:
                self._local.value = None
        return self._local.value
    
    def set(self, value):
        """Set thread-local value."""
        self._local.value = value

class PriorityBarrier:
    """Barrier with priority support."""
    
    def __init__(self, parties):
        self.parties = parties
        self.count = 0
        self.priority_queue = []
        self.condition = threading.Condition()
        self.broken = False
        self.generation = 0
    
    def wait(self, priority=0):
        """Wait at barrier with priority."""
        with self.condition:
            if self.broken:
                raise BrokenBarrierError()
            
            generation = self.generation
            self.count += 1
            self.priority_queue.append((priority, threading.current_thread().ident))
            
            if self.count == self.parties:
                # All threads arrived, release in priority order
                self.priority_queue.sort(key=lambda x: x[0])
                self.generation += 1
                self.count = 0
                self.priority_queue.clear()
                self.condition.notify_all()
                return 0  # Leader
            else:
                # Wait for others
                while generation == self.generation and not self.broken:
                    self.condition.wait()
                
                if self.broken:
                    raise BrokenBarrierError()
                
                # Return position based on priority
                return self.priority_queue.index(
                    (priority, threading.current_thread().ident)
                ) + 1

def demonstrate_reader_writer_lock():
    """Demonstrate reader-writer lock usage."""
    print(f"\n{'='*60}")
    print("READER-WRITER LOCK DEMONSTRATION")
    print(f"{'='*60}")
    
    rwlock = ReaderWriterLock()
    shared_data = {'value': 0, 'reads': 0}
    results = []
    
    def reader(reader_id, iterations):
        """Reader thread function."""
        local_reads = 0
        for _ in range(iterations):
            with rwlock.read_lock():
                # Multiple readers can read simultaneously
                value = shared_data['value']
                shared_data['reads'] += 1
                local_reads += 1
                print(f"  Reader {reader_id}: Read value {value}")
                time.sleep(0.01)  # Simulate read work
        
        results.append(('reader', reader_id, local_reads))
    
    def writer(writer_id, iterations):
        """Writer thread function."""
        local_writes = 0
        for i in range(iterations):
            with rwlock.write_lock():
                # Only one writer at a time
                old_value = shared_data['value']
                shared_data['value'] += 1
                local_writes += 1
                print(f"  Writer {writer_id}: Updated {old_value} → {shared_data['value']}")
                time.sleep(0.02)  # Simulate write work
        
        results.append(('writer', writer_id, local_writes))
    
    # Start readers and writers
    threads = []
    
    # More readers than writers (typical pattern)
    for i in range(5):
        t = threading.Thread(target=reader, args=(i, 3))
        threads.append(t)
        t.start()
    
    for i in range(2):
        t = threading.Thread(target=writer, args=(i, 5))
        threads.append(t)
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    print(f"\nFinal state:")
    print(f"  Value: {shared_data['value']}")
    print(f"  Total reads: {shared_data['reads']}")
    print(f"  Read/Write ratio: {shared_data['reads']/shared_data['value']:.1f}:1")

def demonstrate_thread_local_storage():
    """Demonstrate thread-local storage patterns."""
    print(f"\n{'='*60}")
    print("THREAD-LOCAL STORAGE DEMONSTRATION")
    print(f"{'='*60}")
    
    # Thread-local context
    context = ThreadLocalContext()
    
    # Database connection simulator
    class DatabaseConnection:
        def __init__(self, thread_id):
            self.thread_id = thread_id
            self.conn_id = f"DB-{thread_id}-{random.randint(1000, 9999)}"
            self.queries = 0
        
        def query(self, sql):
            self.queries += 1
            return f"Result from {self.conn_id}"
    
    # Initialize with connection factory
    context.initialize(lambda: DatabaseConnection(threading.current_thread().ident))
    
    results = {}
    
    def worker(worker_id):
        """Worker using thread-local connection."""
        # Get thread-local connection
        conn = context.get()
        
        # Perform work
        for i in range(5):
            result = conn.query(f"SELECT * FROM table WHERE id={i}")
            time.sleep(0.01)
        
        # Store results
        results[worker_id] = {
            'connection': conn.conn_id,
            'queries': conn.queries,
            'thread': threading.current_thread().ident
        }
        
        print(f"  Worker {worker_id}: Used connection {conn.conn_id}, "
              f"executed {conn.queries} queries")
    
    # Start workers
    threads = []
    for i in range(4):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    print(f"\nThread-local connections used:")
    for worker_id, info in results.items():
        print(f"  Worker {worker_id}: {info['connection']} "
              f"(Thread {info['thread']})")
    
    print("\n✓ Each thread has its own connection")
    print("✓ No synchronization needed for thread-local data")

def demonstrate_priority_barrier():
    """Demonstrate priority-based barrier synchronization."""
    print(f"\n{'='*60}")
    print("PRIORITY BARRIER DEMONSTRATION")
    print(f"{'='*60}")
    
    num_threads = 5
    barrier = PriorityBarrier(num_threads)
    results = []
    
    def phased_worker(worker_id, priority):
        """Worker that goes through phases with priority."""
        # Phase 1
        print(f"  Worker {worker_id} (priority {priority}): Starting phase 1")
        time.sleep(random.uniform(0.1, 0.3))
        
        # Wait at barrier with priority
        position = barrier.wait(priority)
        
        # Phase 2 - higher priority threads go first
        print(f"  Worker {worker_id}: Phase 2 (position {position})")
        results.append((worker_id, priority, position))
    
    # Start workers with different priorities
    threads = []
    priorities = [1, 3, 2, 5, 4]  # Different priorities
    
    start_time = time.time()
    
    for i in range(num_threads):
        t = threading.Thread(
            target=phased_worker,
            args=(i, priorities[i])
        )
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    print(f"\nBarrier release order (by priority):")
    results.sort(key=lambda x: x[2])  # Sort by position
    for worker_id, priority, position in results:
        print(f"  Position {position}: Worker {worker_id} (priority {priority})")

def demonstrate_custom_semaphore():
    """Demonstrate custom semaphore with timeout and statistics."""
    print(f"\n{'='*60}")
    print("CUSTOM SEMAPHORE WITH STATISTICS")
    print(f"{'='*60}")
    
    class InstrumentedSemaphore:
        """Semaphore that tracks usage statistics."""
        
        def __init__(self, value):
            self._semaphore = threading.Semaphore(value)
            self._max_value = value
            self._stats = {
                'acquisitions': 0,
                'releases': 0,
                'wait_times': [],
                'timeouts': 0
            }
            self._lock = threading.Lock()
        
        def acquire(self, timeout=None):
            """Acquire with timeout and statistics."""
            start = time.time()
            acquired = self._semaphore.acquire(timeout=timeout)
            wait_time = time.time() - start
            
            with self._lock:
                if acquired:
                    self._stats['acquisitions'] += 1
                    self._stats['wait_times'].append(wait_time)
                else:
                    self._stats['timeouts'] += 1
            
            return acquired
        
        def release(self):
            """Release semaphore."""
            self._semaphore.release()
            with self._lock:
                self._stats['releases'] += 1
        
        def get_stats(self):
            """Get usage statistics."""
            with self._lock:
                if self._stats['wait_times']:
                    avg_wait = sum(self._stats['wait_times']) / len(self._stats['wait_times'])
                    max_wait = max(self._stats['wait_times'])
                else:
                    avg_wait = max_wait = 0
                
                return {
                    'acquisitions': self._stats['acquisitions'],
                    'releases': self._stats['releases'],
                    'timeouts': self._stats['timeouts'],
                    'avg_wait_time': avg_wait,
                    'max_wait_time': max_wait,
                    'utilization': self._stats['acquisitions'] / 
                                  (self._stats['acquisitions'] + self._stats['timeouts'])
                                  if self._stats['acquisitions'] + self._stats['timeouts'] > 0
                                  else 0
                }
    
    # Simulate resource pool with limited capacity
    resource_pool = InstrumentedSemaphore(3)  # 3 resources
    
    def use_resource(user_id):
        """Simulate resource usage."""
        acquired = resource_pool.acquire(timeout=1.0)
        
        if acquired:
            print(f"  User {user_id}: Acquired resource")
            time.sleep(random.uniform(0.2, 0.5))  # Use resource
            resource_pool.release()
            print(f"  User {user_id}: Released resource")
        else:
            print(f"  User {user_id}: Timeout waiting for resource")
    
    # Create many users competing for resources
    threads = []
    for i in range(10):
        t = threading.Thread(target=use_resource, args=(i,))
        threads.append(t)
        t.start()
        time.sleep(0.05)  # Stagger starts
    
    for t in threads:
        t.join()
    
    # Show statistics
    stats = resource_pool.get_stats()
    print(f"\nResource pool statistics:")
    print(f"  Successful acquisitions: {stats['acquisitions']}")
    print(f"  Timeouts: {stats['timeouts']}")
    print(f"  Average wait time: {stats['avg_wait_time']*1000:.1f} ms")
    print(f"  Maximum wait time: {stats['max_wait_time']*1000:.1f} ms")
    print(f"  Utilization: {stats['utilization']*100:.1f}%")

def main():
    """Run all demonstrations."""
    print("Advanced Synchronization Primitives")
    print("=" * 60)
    
    try:
        demonstrate_reader_writer_lock()
        demonstrate_thread_local_storage()
        demonstrate_priority_barrier()
        demonstrate_custom_semaphore()
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("KEY TAKEAWAYS")
    print(f"{'='*60}")
    print("1. Reader-writer locks optimize for read-heavy workloads")
    print("2. Thread-local storage eliminates synchronization needs")
    print("3. Priority barriers enable ordered phase transitions")
    print("4. Custom synchronization primitives can track metrics")
    print("5. Choose the right primitive for your access pattern")

if __name__ == "__main__":
    main()
