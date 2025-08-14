#!/usr/bin/env python3
"""
Multiprocessing Basics: Understanding process-based parallelism

This script demonstrates fundamental multiprocessing concepts and compares
performance with threading for CPU-bound tasks.
"""

import multiprocessing as mp
import threading
import time
import os
import sys
import psutil
from datetime import datetime
import math
import numpy as np
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

class ProcessingDemo:
    """Demonstrates basic multiprocessing concepts."""
    
    def __init__(self):
        self.cpu_count = mp.cpu_count()
        print(f"System Information:")
        print(f"  CPU cores: {self.cpu_count}")
        print(f"  Python version: {sys.version.split()[0]}")
        print(f"  Process ID: {os.getpid()}")
        print(f"  Main thread: {threading.current_thread().name}")
    
    @staticmethod
    def cpu_intensive_task(n, task_id=0):
        """CPU-intensive task: Calculate prime numbers and their sum."""
        process_id = os.getpid()
        thread_id = threading.current_thread().ident
        
        print(f"  Task {task_id} started in process {process_id}, thread {thread_id}")
        
        def is_prime(num):
            if num < 2:
                return False
            for i in range(2, int(math.sqrt(num)) + 1):
                if num % i == 0:
                    return False
            return True
        
        primes = []
        prime_sum = 0
        
        for i in range(2, n):
            if is_prime(i):
                primes.append(i)
                prime_sum += i
        
        return {
            'task_id': task_id,
            'process_id': process_id,
            'thread_id': thread_id,
            'primes_found': len(primes),
            'prime_sum': prime_sum,
            'largest_prime': primes[-1] if primes else None
        }
    
    def compare_sequential_vs_parallel(self, work_size=10000, num_tasks=4):
        """Compare sequential vs parallel execution."""
        print(f"\n{'='*60}")
        print("SEQUENTIAL VS PARALLEL COMPARISON")
        print(f"{'='*60}")
        print(f"Work: Find primes up to {work_size}")
        print(f"Tasks: {num_tasks}")
        
        results = {}
        
        # Sequential execution
        print("\n1. Sequential Execution (single process, single thread)")
        start = time.perf_counter()
        seq_results = []
        for i in range(num_tasks):
            result = self.cpu_intensive_task(work_size, i)
            seq_results.append(result)
        seq_time = time.perf_counter() - start
        results['Sequential'] = seq_time
        
        print(f"   Time: {seq_time:.2f} seconds")
        print(f"   All tasks ran in process: {seq_results[0]['process_id']}")
        
        # Multi-threading
        print("\n2. Multi-Threading (single process, multiple threads)")
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=num_tasks) as executor:
            thread_futures = [
                executor.submit(self.cpu_intensive_task, work_size, i)
                for i in range(num_tasks)
            ]
            thread_results = [f.result() for f in thread_futures]
        thread_time = time.perf_counter() - start
        results['Threading'] = thread_time
        
        print(f"   Time: {thread_time:.2f} seconds")
        print(f"   Speedup vs sequential: {seq_time/thread_time:.2f}x")
        unique_processes = set(r['process_id'] for r in thread_results)
        print(f"   Unique processes used: {len(unique_processes)}")
        
        # Multi-processing
        print("\n3. Multi-Processing (multiple processes)")
        start = time.perf_counter()
        with ProcessPoolExecutor(max_workers=num_tasks) as executor:
            proc_futures = [
                executor.submit(self.cpu_intensive_task, work_size, i)
                for i in range(num_tasks)
            ]
            proc_results = [f.result() for f in proc_futures]
        proc_time = time.perf_counter() - start
        results['Multiprocessing'] = proc_time
        
        print(f"   Time: {proc_time:.2f} seconds")
        print(f"   Speedup vs sequential: {seq_time/proc_time:.2f}x")
        unique_processes = set(r['process_id'] for r in proc_results)
        print(f"   Unique processes used: {len(unique_processes)}")
        
        # Show process distribution
        print("\n   Process distribution:")
        for result in proc_results[:4]:  # Show first 4
            print(f"     Task {result['task_id']}: Process {result['process_id']}")
        
        return results
    
    def demonstrate_process_creation(self):
        """Demonstrate different ways to create processes."""
        print(f"\n{'='*60}")
        print("PROCESS CREATION METHODS")
        print(f"{'='*60}")
        
        # Method 1: Process class
        print("\n1. Using Process class:")
        
        def worker_function(name, duration):
            print(f"   Worker {name} started in process {os.getpid()}")
            time.sleep(duration)
            print(f"   Worker {name} finished")
            return f"Result from {name}"
        
        # Create and start process
        p = mp.Process(target=worker_function, args=("A", 1))
        p.start()
        print(f"   Created process with PID: {p.pid}")
        p.join()
        print(f"   Process exit code: {p.exitcode}")
        
        # Method 2: Process with return value (using Queue)
        print("\n2. Getting return values with Queue:")
        
        def worker_with_queue(name, duration, queue):
            result = worker_function(name, duration)
            queue.put(result)
        
        queue = mp.Queue()
        p = mp.Process(target=worker_with_queue, args=("B", 0.5, queue))
        p.start()
        p.join()
        
        result = queue.get()
        print(f"   Retrieved result: {result}")
        
        # Method 3: Process Pool
        print("\n3. Using Process Pool:")
        
        with mp.Pool(processes=3) as pool:
            # Map
            inputs = [1000, 2000, 3000]
            results = pool.map(self.cpu_intensive_task, inputs)
            print(f"   Pool map results: {[r['primes_found'] for r in results]} primes found")
            
            # Apply async
            async_result = pool.apply_async(self.cpu_intensive_task, (5000,))
            print(f"   Async result: {async_result.get()['primes_found']} primes found")
    
    def demonstrate_process_info(self):
        """Show detailed process information."""
        print(f"\n{'='*60}")
        print("PROCESS INFORMATION AND MONITORING")
        print(f"{'='*60}")
        
        current_process = mp.current_process()
        print(f"\nCurrent process info:")
        print(f"  Name: {current_process.name}")
        print(f"  PID: {current_process.pid}")
        print(f"  Daemon: {current_process.daemon}")
        
        # Using psutil for more details
        try:
            p = psutil.Process()
            
            print(f"\nDetailed process information:")
            print(f"  PID: {p.pid}")
            print(f"  Name: {p.name()}")
            print(f"  Status: {p.status()}")
            print(f"  Created: {datetime.fromtimestamp(p.create_time())}")
            print(f"  CPU count: {p.cpu_affinity() if hasattr(p, 'cpu_affinity') else 'N/A'}")
            print(f"  Memory (RSS): {p.memory_info().rss / 1024 / 1024:.1f} MB")
            print(f"  Memory (VMS): {p.memory_info().vms / 1024 / 1024:.1f} MB")
            print(f"  CPU percent: {p.cpu_percent(interval=0.1)}%")
            print(f"  Threads: {p.num_threads()}")
            
            # Show parent/child relationships
            print(f"\nProcess relationships:")
            print(f"  Parent PID: {p.ppid()}")
            
            children = p.children()
            if children:
                print(f"  Children: {[child.pid for child in children]}")
            else:
                print(f"  Children: None")
            
        except Exception as e:
            print(f"  Error getting process info: {e}")
    
    def measure_process_overhead(self):
        """Measure the overhead of process creation."""
        print(f"\n{'='*60}")
        print("PROCESS CREATION OVERHEAD")
        print(f"{'='*60}")
        
        iterations = 50
        
        # Measure thread creation
        print(f"\nMeasuring thread creation ({iterations} iterations)...")
        start = time.perf_counter()
        for _ in range(iterations):
            t = threading.Thread(target=lambda: None)
            t.start()
            t.join()
        thread_time = time.perf_counter() - start
        thread_avg = thread_time / iterations * 1000
        
        # Measure process creation (spawn)
        print(f"Measuring process creation - spawn ({iterations} iterations)...")
        ctx = mp.get_context('spawn')
        start = time.perf_counter()
        for _ in range(iterations):
            p = ctx.Process(target=lambda: None)
            p.start()
            p.join()
        spawn_time = time.perf_counter() - start
        spawn_avg = spawn_time / iterations * 1000
        
        # Measure process creation (fork) - Unix only
        if hasattr(os, 'fork'):
            print(f"Measuring process creation - fork ({iterations} iterations)...")
            ctx = mp.get_context('fork')
            start = time.perf_counter()
            for _ in range(iterations):
                p = ctx.Process(target=lambda: None)
                p.start()
                p.join()
            fork_time = time.perf_counter() - start
            fork_avg = fork_time / iterations * 1000
        else:
            fork_avg = None
        
        print(f"\nResults:")
        print(f"  Thread creation: {thread_avg:.2f} ms average")
        print(f"  Process creation (spawn): {spawn_avg:.2f} ms average")
        if fork_avg:
            print(f"  Process creation (fork): {fork_avg:.2f} ms average")
        print(f"  Spawn overhead vs thread: {spawn_avg/thread_avg:.1f}x slower")
        if fork_avg:
            print(f"  Fork overhead vs thread: {fork_avg/thread_avg:.1f}x slower")
    
    def demonstrate_shared_state_problem(self):
        """Show the difference in shared state between threads and processes."""
        print(f"\n{'='*60}")
        print("SHARED STATE: THREADS VS PROCESSES")
        print(f"{'='*60}")
        
        # Global variable
        global_counter = 0
        
        def increment_global(iterations):
            global global_counter
            for _ in range(iterations):
                global_counter += 1
            return global_counter
        
        iterations = 1000
        
        # Test with threads
        print("\n1. Threads (shared memory):")
        global_counter = 0
        
        threads = []
        for i in range(3):
            t = threading.Thread(target=increment_global, args=(iterations,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        print(f"   Final counter value: {global_counter}")
        print(f"   Expected: {3 * iterations}")
        print("   ✓ Threads share the same global variable")
        
        # Test with processes
        print("\n2. Processes (separate memory):")
        global_counter = 0
        
        processes = []
        for i in range(3):
            p = mp.Process(target=increment_global, args=(iterations,))
            processes.append(p)
            p.start()
        
        for p in processes:
            p.join()
        
        print(f"   Final counter value: {global_counter}")
        print(f"   Expected: {3 * iterations}")
        print("   ✗ Each process has its own copy of global variables")
        
        # Correct way with shared memory
        print("\n3. Processes with shared memory (correct way):")
        
        def increment_shared(shared_counter, lock, iterations):
            for _ in range(iterations):
                with lock:
                    shared_counter.value += 1
        
        shared_counter = mp.Value('i', 0)
        lock = mp.Lock()
        
        processes = []
        for i in range(3):
            p = mp.Process(target=increment_shared, 
                          args=(shared_counter, lock, iterations))
            processes.append(p)
            p.start()
        
        for p in processes:
            p.join()
        
        print(f"   Final counter value: {shared_counter.value}")
        print(f"   Expected: {3 * iterations}")
        print("   ✓ Shared memory allows processes to share state")

def visualize_performance(results):
    """Create a simple text-based performance visualization."""
    print(f"\n{'='*60}")
    print("PERFORMANCE VISUALIZATION")
    print(f"{'='*60}")
    
    if not results:
        return
    
    # Find the maximum time for scaling
    max_time = max(results.values())
    bar_width = 40
    
    print(f"\n{'Method':<20} {'Time (s)':<10} Performance")
    print("-" * 70)
    
    for method, time_taken in results.items():
        # Calculate bar length
        bar_length = int((time_taken / max_time) * bar_width)
        bar = '█' * bar_length
        
        # Calculate relative performance
        baseline = results.get('Sequential', time_taken)
        speedup = baseline / time_taken
        
        print(f"{method:<20} {time_taken:<10.2f} {bar} ({speedup:.2f}x)")

def main():
    """Run multiprocessing demonstrations."""
    print("Multiprocessing Basics Demonstration")
    print("=" * 60)
    
    demo = ProcessingDemo()
    
    try:
        # 1. Process creation methods
        demo.demonstrate_process_creation()
        
        # 2. Process information
        demo.demonstrate_process_info()
        
        # 3. Compare performance
        results = demo.compare_sequential_vs_parallel()
        visualize_performance(results)
        
        # 4. Measure overhead
        demo.measure_process_overhead()
        
        # 5. Shared state demonstration
        demo.demonstrate_shared_state_problem()
        
        # Summary
        print(f"\n{'='*60}")
        print("KEY INSIGHTS")
        print(f"{'='*60}")
        print("1. Multiprocessing provides true parallelism for CPU-bound tasks")
        print("2. Each process has its own memory space and GIL")
        print("3. Process creation has significant overhead vs threads")
        print("4. Use process pools to minimize creation overhead")
        print("5. Shared state requires explicit IPC mechanisms")
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Ensure proper cleanup on Windows
    mp.freeze_support()
    main()
