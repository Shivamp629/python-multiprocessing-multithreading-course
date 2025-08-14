#!/usr/bin/env python3
"""
Thread Pool Patterns: Efficient concurrent execution

This script demonstrates various thread pool implementations and patterns
for managing concurrent tasks efficiently.
"""

import threading
import queue
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import sys
import psutil
import os

class Task:
    """Represents a task to be executed."""
    
    def __init__(self, task_id, task_type='compute', duration=None):
        self.task_id = task_id
        self.task_type = task_type
        self.duration = duration or random.uniform(0.1, 0.5)
        self.start_time = None
        self.end_time = None
        self.result = None
        self.thread_id = None
        self.error = None
    
    def execute(self):
        """Execute the task."""
        self.start_time = time.perf_counter()
        self.thread_id = threading.current_thread().ident
        
        try:
            if self.task_type == 'compute':
                # Simulate CPU work
                result = 0
                for i in range(int(self.duration * 1000000)):
                    result += i % 1000
                self.result = result
            
            elif self.task_type == 'io':
                # Simulate I/O wait
                time.sleep(self.duration)
                self.result = f"I/O completed after {self.duration:.2f}s"
            
            elif self.task_type == 'mixed':
                # Mix of CPU and I/O
                time.sleep(self.duration / 2)
                result = sum(range(int(self.duration * 500000)))
                time.sleep(self.duration / 2)
                self.result = result
            
            elif self.task_type == 'error':
                # Simulate an error
                raise Exception(f"Task {self.task_id} failed intentionally")
            
        except Exception as e:
            self.error = str(e)
        finally:
            self.end_time = time.perf_counter()
    
    @property
    def execution_time(self):
        """Get task execution time."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

class CustomThreadPool:
    """Custom thread pool implementation."""
    
    def __init__(self, num_workers=4, queue_size=100):
        self.num_workers = num_workers
        self.task_queue = queue.Queue(maxsize=queue_size)
        self.workers = []
        self.shutdown_event = threading.Event()
        self.statistics = {
            'tasks_submitted': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'total_execution_time': 0,
        }
        self.stats_lock = threading.Lock()
        
    def start(self):
        """Start the thread pool."""
        for i in range(self.num_workers):
            worker = threading.Thread(
                target=self._worker,
                name=f"PoolWorker-{i}",
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
        
        print(f"Custom thread pool started with {self.num_workers} workers")
    
    def _worker(self):
        """Worker thread function."""
        while not self.shutdown_event.is_set():
            try:
                # Get task with timeout to check shutdown
                task = self.task_queue.get(timeout=0.1)
                
                # Execute task
                task.execute()
                
                # Update statistics
                with self.stats_lock:
                    self.statistics['tasks_completed'] += 1
                    if task.error:
                        self.statistics['tasks_failed'] += 1
                    if task.execution_time:
                        self.statistics['total_execution_time'] += task.execution_time
                
                # Mark task as done
                self.task_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Worker error: {e}")
    
    def submit(self, task):
        """Submit a task to the pool."""
        with self.stats_lock:
            self.statistics['tasks_submitted'] += 1
        self.task_queue.put(task)
    
    def shutdown(self, wait=True):
        """Shutdown the thread pool."""
        self.shutdown_event.set()
        
        if wait:
            # Wait for all tasks to complete
            self.task_queue.join()
            
            # Wait for workers to finish
            for worker in self.workers:
                worker.join()
        
        print("Custom thread pool shut down")
    
    def get_statistics(self):
        """Get pool statistics."""
        with self.stats_lock:
            stats = self.statistics.copy()
            stats['queue_size'] = self.task_queue.qsize()
            stats['active_workers'] = sum(1 for w in self.workers if w.is_alive())
            return stats

class ThreadPoolComparison:
    """Compare different thread pool implementations."""
    
    def __init__(self):
        self.results = {}
    
    def benchmark_custom_pool(self, num_tasks=100, num_workers=4):
        """Benchmark custom thread pool."""
        print(f"\n{'='*60}")
        print("CUSTOM THREAD POOL BENCHMARK")
        print(f"{'='*60}")
        
        pool = CustomThreadPool(num_workers=num_workers)
        pool.start()
        
        tasks = []
        start_time = time.perf_counter()
        
        # Submit tasks
        for i in range(num_tasks):
            task_type = random.choice(['compute', 'io', 'mixed'])
            task = Task(i, task_type, duration=random.uniform(0.01, 0.1))
            tasks.append(task)
            pool.submit(task)
        
        # Wait for completion
        pool.shutdown(wait=True)
        
        end_time = time.perf_counter()
        total_time = end_time - start_time
        
        # Analyze results
        completed = sum(1 for t in tasks if t.result is not None)
        failed = sum(1 for t in tasks if t.error is not None)
        
        stats = pool.get_statistics()
        
        print(f"\nResults:")
        print(f"  Total time: {total_time:.2f} seconds")
        print(f"  Tasks completed: {completed}/{num_tasks}")
        print(f"  Tasks failed: {failed}")
        print(f"  Throughput: {completed/total_time:.1f} tasks/second")
        print(f"  Average task time: {stats['total_execution_time']/completed:.3f} seconds")
        
        self.results['Custom Pool'] = {
            'total_time': total_time,
            'throughput': completed/total_time,
            'completed': completed
        }
        
        return tasks
    
    def benchmark_concurrent_futures(self, num_tasks=100, num_workers=4):
        """Benchmark concurrent.futures ThreadPoolExecutor."""
        print(f"\n{'='*60}")
        print("CONCURRENT.FUTURES THREADPOOLEXECUTOR BENCHMARK")
        print(f"{'='*60}")
        
        tasks = []
        futures = []
        
        start_time = time.perf_counter()
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit tasks
            for i in range(num_tasks):
                task_type = random.choice(['compute', 'io', 'mixed'])
                task = Task(i, task_type, duration=random.uniform(0.01, 0.1))
                tasks.append(task)
                future = executor.submit(task.execute)
                futures.append((future, task))
            
            # Wait for completion
            completed = 0
            for future, task in futures:
                try:
                    future.result()
                    completed += 1
                except Exception as e:
                    print(f"Task {task.task_id} failed: {e}")
        
        end_time = time.perf_counter()
        total_time = end_time - start_time
        
        print(f"\nResults:")
        print(f"  Total time: {total_time:.2f} seconds")
        print(f"  Tasks completed: {completed}/{num_tasks}")
        print(f"  Throughput: {completed/total_time:.1f} tasks/second")
        
        self.results['ThreadPoolExecutor'] = {
            'total_time': total_time,
            'throughput': completed/total_time,
            'completed': completed
        }
        
        return tasks
    
    def demonstrate_patterns(self):
        """Demonstrate various thread pool usage patterns."""
        print(f"\n{'='*60}")
        print("THREAD POOL PATTERNS")
        print(f"{'='*60}")
        
        # Pattern 1: Map pattern
        print("\n1. Map Pattern - Apply function to multiple inputs")
        with ThreadPoolExecutor(max_workers=4) as executor:
            inputs = range(10)
            
            def process_item(x):
                time.sleep(0.1)
                return x * x
            
            start = time.perf_counter()
            results = list(executor.map(process_item, inputs))
            elapsed = time.perf_counter() - start
            
            print(f"   Processed {len(results)} items in {elapsed:.2f} seconds")
            print(f"   Results: {results}")
        
        # Pattern 2: As completed pattern
        print("\n2. As-Completed Pattern - Process results as they finish")
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {}
            
            # Submit tasks with different durations
            for i in range(5):
                duration = random.uniform(0.1, 0.5)
                future = executor.submit(time.sleep, duration)
                futures[future] = (i, duration)
            
            print("   Tasks submitted, processing as they complete:")
            for future in as_completed(futures):
                task_id, duration = futures[future]
                print(f"   Task {task_id} completed (duration: {duration:.2f}s)")
        
        # Pattern 3: Callback pattern
        print("\n3. Callback Pattern - Attach callbacks to futures")
        
        results_queue = queue.Queue()
        
        def task_with_callback(task_id, duration):
            time.sleep(duration)
            return f"Task {task_id} result"
        
        def handle_result(future):
            try:
                result = future.result()
                results_queue.put(('success', result))
            except Exception as e:
                results_queue.put(('error', str(e)))
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            for i in range(5):
                future = executor.submit(task_with_callback, i, random.uniform(0.1, 0.3))
                future.add_done_callback(handle_result)
            
            # Collect results
            time.sleep(0.5)
            print("   Callback results:")
            while not results_queue.empty():
                status, result = results_queue.get()
                print(f"   {status}: {result}")
        
        # Pattern 4: Rate limiting
        print("\n4. Rate Limiting Pattern - Control submission rate")
        
        class RateLimitedExecutor:
            def __init__(self, executor, rate_limit):
                self.executor = executor
                self.rate_limit = rate_limit  # tasks per second
                self.last_submit = 0
            
            def submit(self, fn, *args, **kwargs):
                # Calculate delay
                now = time.time()
                time_since_last = now - self.last_submit
                min_interval = 1.0 / self.rate_limit
                
                if time_since_last < min_interval:
                    time.sleep(min_interval - time_since_last)
                
                self.last_submit = time.time()
                return self.executor.submit(fn, *args, **kwargs)
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            rate_limited = RateLimitedExecutor(executor, rate_limit=5)  # 5 tasks/second
            
            print("   Submitting 10 tasks with rate limit of 5/second")
            start = time.perf_counter()
            
            futures = []
            for i in range(10):
                future = rate_limited.submit(lambda x: x, i)
                futures.append(future)
            
            # Wait for completion
            for f in futures:
                f.result()
            
            elapsed = time.perf_counter() - start
            print(f"   Completed in {elapsed:.2f} seconds (expected ~2 seconds)")

def monitor_thread_pool_health():
    """Monitor thread pool health and resource usage."""
    print(f"\n{'='*60}")
    print("THREAD POOL HEALTH MONITORING")
    print(f"{'='*60}")
    
    process = psutil.Process()
    
    def get_thread_info():
        threads = process.threads()
        return {
            'count': len(threads),
            'cpu_times': sum(t.user_time + t.system_time for t in threads),
        }
    
    # Create a pool with monitoring
    print("Creating thread pool and monitoring resource usage...")
    
    initial_threads = get_thread_info()
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit some tasks
        futures = []
        for i in range(50):
            future = executor.submit(time.sleep, random.uniform(0.1, 0.5))
            futures.append(future)
        
        # Monitor while running
        time.sleep(0.1)  # Let threads start
        
        active_threads = get_thread_info()
        active_memory = process.memory_info().rss / 1024 / 1024
        
        print(f"\nThread pool statistics:")
        print(f"  Initial threads: {initial_threads['count']}")
        print(f"  Active threads: {active_threads['count']}")
        print(f"  Thread overhead: {active_threads['count'] - initial_threads['count']} threads")
        print(f"  Memory usage: {active_memory:.1f} MB (delta: {active_memory - initial_memory:+.1f} MB)")
        
        # Wait for some tasks to complete
        completed = sum(1 for f in futures if f.done())
        print(f"  Tasks completed: {completed}/50")
        
        # Get executor internal state (if available)
        if hasattr(executor, '_threads'):
            print(f"  Executor threads: {len(executor._threads)}")
        if hasattr(executor, '_work_queue'):
            print(f"  Work queue size: {executor._work_queue.qsize()}")

def main():
    """Run thread pool demonstrations."""
    print("Thread Pool Patterns and Best Practices")
    print(f"Python version: {sys.version}")
    print(f"CPU count: {os.cpu_count()}")
    
    try:
        # Compare implementations
        comparison = ThreadPoolComparison()
        
        # Run benchmarks
        num_tasks = 50
        num_workers = 4
        
        comparison.benchmark_custom_pool(num_tasks, num_workers)
        comparison.benchmark_concurrent_futures(num_tasks, num_workers)
        
        # Show comparison
        print(f"\n{'='*60}")
        print("PERFORMANCE COMPARISON")
        print(f"{'='*60}")
        print(f"{'Implementation':<20} {'Time (s)':<10} {'Throughput':<15} {'Completed':<10}")
        print("-" * 60)
        
        for name, results in comparison.results.items():
            print(f"{name:<20} {results['total_time']:<10.2f} "
                  f"{results['throughput']:<15.1f} {results['completed']:<10}")
        
        # Demonstrate patterns
        comparison.demonstrate_patterns()
        
        # Monitor health
        monitor_thread_pool_health()
        
        # Best practices summary
        print(f"\n{'='*60}")
        print("THREAD POOL BEST PRACTICES")
        print(f"{'='*60}")
        print("1. Use ThreadPoolExecutor for simple cases")
        print("2. Set max_workers based on I/O vs CPU workload")
        print("3. Use as_completed() for processing results ASAP")
        print("4. Implement proper error handling with futures")
        print("5. Monitor queue size to prevent memory issues")
        print("6. Use rate limiting for external API calls")
        print("7. Gracefully shutdown pools to avoid losing work")
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
