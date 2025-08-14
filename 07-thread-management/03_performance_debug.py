#!/usr/bin/env python3
"""
Thread Performance and Debugging

This script demonstrates performance analysis and debugging techniques
for multi-threaded applications.
"""

import threading
import time
import sys
import os
import queue
import psutil
from collections import defaultdict, deque
from contextlib import contextmanager
from datetime import datetime
import traceback
import signal

class ThreadProfiler:
    """Profile thread execution and lock contention."""
    
    def __init__(self):
        self.thread_stats = defaultdict(lambda: {
            'cpu_time': 0,
            'wait_time': 0,
            'execution_count': 0,
            'lock_acquisitions': 0,
            'lock_wait_time': 0
        })
        self.lock_stats = defaultdict(lambda: {
            'acquisitions': 0,
            'contention_time': 0,
            'holders': defaultdict(int),
            'waiters': defaultdict(int)
        })
        self._lock = threading.Lock()
    
    @contextmanager
    def profile_thread(self, name=None):
        """Profile thread execution time."""
        thread = threading.current_thread()
        thread_name = name or thread.name
        
        start_cpu = time.thread_time() if hasattr(time, 'thread_time') else time.time()
        start_wall = time.time()
        
        yield
        
        end_cpu = time.thread_time() if hasattr(time, 'thread_time') else time.time()
        end_wall = time.time()
        
        with self._lock:
            stats = self.thread_stats[thread_name]
            stats['cpu_time'] += (end_cpu - start_cpu)
            stats['execution_count'] += 1
    
    def create_profiled_lock(self, name):
        """Create a lock that tracks contention."""
        return ProfiledLock(name, self)
    
    def get_report(self):
        """Generate profiling report."""
        with self._lock:
            report = ["Thread Performance Report", "=" * 50]
            
            # Thread statistics
            report.append("\nThread Statistics:")
            for thread_name, stats in self.thread_stats.items():
                report.append(f"\n{thread_name}:")
                report.append(f"  CPU time: {stats['cpu_time']*1000:.1f} ms")
                report.append(f"  Executions: {stats['execution_count']}")
                if stats['lock_acquisitions'] > 0:
                    report.append(f"  Lock acquisitions: {stats['lock_acquisitions']}")
                    report.append(f"  Lock wait time: {stats['lock_wait_time']*1000:.1f} ms")
            
            # Lock statistics
            if self.lock_stats:
                report.append("\nLock Contention Statistics:")
                for lock_name, stats in self.lock_stats.items():
                    report.append(f"\n{lock_name}:")
                    report.append(f"  Total acquisitions: {stats['acquisitions']}")
                    report.append(f"  Total contention time: {stats['contention_time']*1000:.1f} ms")
                    report.append(f"  Average wait: {stats['contention_time']/stats['acquisitions']*1000:.2f} ms")
            
            return "\n".join(report)

class ProfiledLock:
    """Lock wrapper that tracks contention."""
    
    def __init__(self, name, profiler):
        self.name = name
        self.profiler = profiler
        self._lock = threading.Lock()
        self._owner = None
    
    def acquire(self, blocking=True, timeout=-1):
        """Acquire lock with profiling."""
        thread = threading.current_thread()
        start = time.time()
        
        # Track waiting
        if self._owner and self._owner != thread:
            with self.profiler._lock:
                self.profiler.lock_stats[self.name]['waiters'][thread.name] += 1
        
        acquired = self._lock.acquire(blocking, timeout)
        wait_time = time.time() - start
        
        if acquired:
            self._owner = thread
            with self.profiler._lock:
                stats = self.profiler.lock_stats[self.name]
                stats['acquisitions'] += 1
                stats['contention_time'] += wait_time
                stats['holders'][thread.name] += 1
                
                thread_stats = self.profiler.thread_stats[thread.name]
                thread_stats['lock_acquisitions'] += 1
                thread_stats['lock_wait_time'] += wait_time
        
        return acquired
    
    def release(self):
        """Release lock."""
        self._owner = None
        self._lock.release()
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

class DeadlockDetector:
    """Detect potential deadlocks in multi-threaded code."""
    
    def __init__(self):
        self.lock_graph = defaultdict(set)  # thread -> locks held
        self.wait_graph = defaultdict(set)  # thread -> locks waiting for
        self._lock = threading.Lock()
        self.deadlocks = []
    
    def acquire_lock(self, lock_id):
        """Record lock acquisition."""
        thread = threading.current_thread()
        with self._lock:
            self.lock_graph[thread.ident].add(lock_id)
            self.wait_graph[thread.ident].discard(lock_id)
            self._check_deadlock()
    
    def wait_for_lock(self, lock_id):
        """Record waiting for a lock."""
        thread = threading.current_thread()
        with self._lock:
            self.wait_graph[thread.ident].add(lock_id)
            self._check_deadlock()
    
    def release_lock(self, lock_id):
        """Record lock release."""
        thread = threading.current_thread()
        with self._lock:
            self.lock_graph[thread.ident].discard(lock_id)
    
    def _check_deadlock(self):
        """Check for circular dependencies."""
        # Build dependency graph
        dependencies = defaultdict(set)
        
        for thread, waiting_locks in self.wait_graph.items():
            for lock in waiting_locks:
                # Find who holds this lock
                for holder, held_locks in self.lock_graph.items():
                    if lock in held_locks:
                        dependencies[thread].add(holder)
        
        # Find cycles using DFS
        def has_cycle(thread, visited, rec_stack):
            visited.add(thread)
            rec_stack.add(thread)
            
            for dep in dependencies.get(thread, []):
                if dep not in visited:
                    if has_cycle(dep, visited, rec_stack):
                        return True
                elif dep in rec_stack:
                    return True
            
            rec_stack.remove(thread)
            return False
        
        visited = set()
        for thread in dependencies:
            if thread not in visited:
                if has_cycle(thread, visited, set()):
                    self.deadlocks.append({
                        'timestamp': datetime.now(),
                        'threads': list(dependencies.keys()),
                        'graph': dict(dependencies)
                    })
                    return True
        
        return False

def demonstrate_performance_profiling():
    """Demonstrate thread performance profiling."""
    print(f"\n{'='*60}")
    print("THREAD PERFORMANCE PROFILING")
    print(f"{'='*60}")
    
    profiler = ThreadProfiler()
    shared_data = {'counter': 0, 'sum': 0}
    
    # Create profiled locks
    data_lock = profiler.create_profiled_lock("data_lock")
    sum_lock = profiler.create_profiled_lock("sum_lock")
    
    def cpu_intensive_task(task_id, iterations):
        """CPU-intensive task with profiling."""
        with profiler.profile_thread(f"CPU-Worker-{task_id}"):
            local_sum = 0
            for i in range(iterations):
                # Simulate CPU work
                local_sum += sum(j * j for j in range(100))
                
                # Occasionally update shared data
                if i % 100 == 0:
                    with data_lock:
                        shared_data['counter'] += 1
                    
                    with sum_lock:
                        shared_data['sum'] += local_sum
                        local_sum = 0
            
            # Final update
            with sum_lock:
                shared_data['sum'] += local_sum
    
    def io_intensive_task(task_id, iterations):
        """I/O-intensive task with profiling."""
        with profiler.profile_thread(f"IO-Worker-{task_id}"):
            for i in range(iterations):
                # Simulate I/O
                time.sleep(0.001)
                
                # Update shared data
                with data_lock:
                    shared_data['counter'] += 1
    
    print("Running profiled tasks...")
    threads = []
    
    # CPU-intensive threads
    for i in range(2):
        t = threading.Thread(target=cpu_intensive_task, args=(i, 1000))
        threads.append(t)
        t.start()
    
    # I/O-intensive threads
    for i in range(2):
        t = threading.Thread(target=io_intensive_task, args=(i, 50))
        threads.append(t)
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    # Show profiling report
    print("\n" + profiler.get_report())

def demonstrate_deadlock_detection():
    """Demonstrate deadlock detection."""
    print(f"\n{'='*60}")
    print("DEADLOCK DETECTION")
    print(f"{'='*60}")
    
    detector = DeadlockDetector()
    
    class DeadlockLock:
        """Lock that reports to deadlock detector."""
        
        def __init__(self, lock_id):
            self.lock_id = lock_id
            self._lock = threading.Lock()
        
        def acquire(self):
            detector.wait_for_lock(self.lock_id)
            self._lock.acquire()
            detector.acquire_lock(self.lock_id)
        
        def release(self):
            self._lock.release()
            detector.release_lock(self.lock_id)
        
        def __enter__(self):
            self.acquire()
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.release()
    
    # Create locks
    lock_a = DeadlockLock("lock_a")
    lock_b = DeadlockLock("lock_b")
    
    def worker1():
        """Worker that acquires A then B."""
        with lock_a:
            print("  Worker 1: Acquired lock A")
            time.sleep(0.1)
            with lock_b:
                print("  Worker 1: Acquired lock B")
    
    def worker2():
        """Worker that acquires B then A (potential deadlock)."""
        with lock_b:
            print("  Worker 2: Acquired lock B")
            time.sleep(0.1)
            with lock_a:
                print("  Worker 2: Acquired lock A")
    
    print("Creating potential deadlock scenario...")
    
    # Start threads
    t1 = threading.Thread(target=worker1)
    t2 = threading.Thread(target=worker2)
    
    # Set timeout to avoid actual deadlock
    t1.start()
    t2.start()
    
    # Monitor for deadlock
    start_time = time.time()
    while t1.is_alive() or t2.is_alive():
        if time.time() - start_time > 2:
            print("\n  ⚠️  Deadlock detected!")
            if detector.deadlocks:
                deadlock = detector.deadlocks[-1]
                print(f"  Threads involved: {deadlock['threads']}")
                print(f"  Dependency graph: {deadlock['graph']}")
            break
        time.sleep(0.1)
    
    # Clean up threads if deadlocked
    if t1.is_alive() or t2.is_alive():
        print("\n  Note: In real scenario, threads would be stuck")
        print("  Prevention: Always acquire locks in same order")

def demonstrate_thread_monitoring():
    """Demonstrate real-time thread monitoring."""
    print(f"\n{'='*60}")
    print("THREAD MONITORING")
    print(f"{'='*60}")
    
    class ThreadMonitor:
        """Monitor thread health and performance."""
        
        def __init__(self):
            self.threads = {}
            self.running = True
            self._lock = threading.Lock()
        
        def register_thread(self, name):
            """Register a thread for monitoring."""
            thread = threading.current_thread()
            with self._lock:
                self.threads[thread.ident] = {
                    'name': name,
                    'thread': thread,
                    'last_heartbeat': time.time(),
                    'status': 'running',
                    'operations': 0
                }
        
        def heartbeat(self):
            """Update thread heartbeat."""
            thread = threading.current_thread()
            with self._lock:
                if thread.ident in self.threads:
                    self.threads[thread.ident]['last_heartbeat'] = time.time()
                    self.threads[thread.ident]['operations'] += 1
        
        def check_health(self):
            """Check thread health."""
            with self._lock:
                current_time = time.time()
                unhealthy = []
                
                for tid, info in self.threads.items():
                    if info['thread'].is_alive():
                        time_since_heartbeat = current_time - info['last_heartbeat']
                        if time_since_heartbeat > 2.0:
                            info['status'] = 'stalled'
                            unhealthy.append(info['name'])
                        else:
                            info['status'] = 'healthy'
                    else:
                        info['status'] = 'dead'
                
                return unhealthy
        
        def get_stats(self):
            """Get thread statistics."""
            with self._lock:
                stats = []
                for tid, info in self.threads.items():
                    stats.append({
                        'name': info['name'],
                        'status': info['status'],
                        'operations': info['operations'],
                        'alive': info['thread'].is_alive()
                    })
                return stats
    
    monitor = ThreadMonitor()
    
    def monitored_worker(worker_id, should_stall=False):
        """Worker that reports to monitor."""
        monitor.register_thread(f"Worker-{worker_id}")
        
        for i in range(20):
            # Simulate work
            time.sleep(0.1)
            monitor.heartbeat()
            
            # Simulate stall
            if should_stall and i == 10:
                print(f"  Worker {worker_id}: Simulating stall...")
                time.sleep(3)
        
        print(f"  Worker {worker_id}: Completed")
    
    def monitor_thread():
        """Thread that monitors other threads."""
        while monitor.running:
            time.sleep(1)
            unhealthy = monitor.check_health()
            
            if unhealthy:
                print(f"\n  ⚠️  Unhealthy threads detected: {unhealthy}")
            
            # Show stats
            stats = monitor.get_stats()
            alive_count = sum(1 for s in stats if s['alive'])
            print(f"  Active threads: {alive_count}, Total operations: {sum(s['operations'] for s in stats)}")
    
    print("Starting monitored workers...")
    
    # Start monitor
    mon_thread = threading.Thread(target=monitor_thread)
    mon_thread.daemon = True
    mon_thread.start()
    
    # Start workers
    workers = []
    for i in range(3):
        # Make worker 1 stall
        t = threading.Thread(target=monitored_worker, args=(i, i == 1))
        t.start()
        workers.append(t)
    
    # Wait for workers
    for t in workers:
        t.join()
    
    monitor.running = False
    
    print("\nFinal thread statistics:")
    for stat in monitor.get_stats():
        print(f"  {stat['name']}: {stat['status']}, {stat['operations']} operations")

def demonstrate_lock_free_counter():
    """Demonstrate lock-free atomic operations."""
    print(f"\n{'='*60}")
    print("LOCK-FREE OPERATIONS")
    print(f"{'='*60}")
    
    import multiprocessing
    
    # Shared memory counter (lock-free)
    shared_counter = multiprocessing.Value('i', 0)
    
    # Traditional locked counter
    locked_counter = 0
    lock = threading.Lock()
    
    iterations = 100000
    num_threads = 4
    
    def increment_atomic(counter, iterations):
        """Increment using atomic operations."""
        for _ in range(iterations):
            with counter.get_lock():
                counter.value += 1
    
    def increment_locked(iterations):
        """Increment using traditional lock."""
        global locked_counter
        for _ in range(iterations):
            with lock:
                locked_counter += 1
    
    # Test atomic counter
    print("Testing atomic counter...")
    start = time.time()
    
    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=increment_atomic, args=(shared_counter, iterations))
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()
    
    atomic_time = time.time() - start
    print(f"  Atomic counter: {shared_counter.value} in {atomic_time:.3f}s")
    
    # Test locked counter
    print("\nTesting locked counter...")
    locked_counter = 0
    start = time.time()
    
    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=increment_locked, args=(iterations,))
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()
    
    locked_time = time.time() - start
    print(f"  Locked counter: {locked_counter} in {locked_time:.3f}s")
    
    print(f"\nPerformance comparison:")
    print(f"  Atomic operations: {num_threads * iterations / atomic_time:,.0f} ops/sec")
    print(f"  Locked operations: {num_threads * iterations / locked_time:,.0f} ops/sec")

def main():
    """Run all performance and debugging demonstrations."""
    print("Thread Performance and Debugging")
    print("=" * 60)
    
    try:
        demonstrate_performance_profiling()
        demonstrate_deadlock_detection()
        demonstrate_thread_monitoring()
        demonstrate_lock_free_counter()
        
    except Exception as e:
        print(f"\nError: {e}")
        traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("PERFORMANCE & DEBUGGING BEST PRACTICES")
    print(f"{'='*60}")
    print("1. Profile to identify lock contention")
    print("2. Use deadlock detection in development")
    print("3. Monitor thread health in production")
    print("4. Consider lock-free algorithms for hot paths")
    print("5. Minimize critical section size")
    print("6. Use appropriate synchronization primitives")

if __name__ == "__main__":
    main()
