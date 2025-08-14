#!/usr/bin/env python3
"""
GIL (Global Interpreter Lock) Demonstration

This script demonstrates how the GIL affects CPU-bound vs I/O-bound operations
in multi-threaded Python programs.
"""

import threading
import time
import sys
import os
import requests
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import matplotlib.pyplot as plt
import numpy as np

# Disable SSL warnings for demo purposes
import urllib3
urllib3.disable_warnings()

class GILDemonstrator:
    """Demonstrates GIL behavior with different workload types."""
    
    def __init__(self):
        self.results = {}
        
    def cpu_bound_task(self, n):
        """CPU-intensive task: Calculate prime numbers."""
        def is_prime(num):
            if num < 2:
                return False
            for i in range(2, int(num ** 0.5) + 1):
                if num % i == 0:
                    return False
            return True
        
        primes = []
        for i in range(n):
            if is_prime(i):
                primes.append(i)
        return len(primes)
    
    def io_bound_task(self, urls):
        """I/O-intensive task: Make HTTP requests."""
        results = []
        for url in urls:
            try:
                response = requests.get(url, timeout=5, verify=False)
                results.append((url, response.status_code, len(response.content)))
            except Exception as e:
                results.append((url, 'error', str(e)))
        return results
    
    def mixed_task(self, n, url):
        """Mixed CPU and I/O task."""
        # CPU part: Calculate primes
        primes = self.cpu_bound_task(n // 10)
        
        # I/O part: Make HTTP request
        try:
            response = requests.get(url, timeout=5, verify=False)
            io_result = (response.status_code, len(response.content))
        except:
            io_result = ('error', 0)
        
        return (primes, io_result)
    
    def run_cpu_bound_comparison(self, task_size=5000, num_tasks=4):
        """Compare CPU-bound task performance across execution models."""
        print("\n" + "="*60)
        print("CPU-BOUND TASK COMPARISON")
        print("="*60)
        print(f"Task: Calculate primes up to {task_size}")
        print(f"Number of tasks: {num_tasks}")
        
        results = {}
        
        # Single-threaded baseline
        print("\n1. Single-threaded execution...")
        start = time.perf_counter()
        for _ in range(num_tasks):
            self.cpu_bound_task(task_size)
        single_time = time.perf_counter() - start
        results['Single Thread'] = single_time
        print(f"   Time: {single_time:.2f} seconds")
        
        # Multi-threaded
        print("\n2. Multi-threaded execution...")
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=num_tasks) as executor:
            futures = [executor.submit(self.cpu_bound_task, task_size) 
                      for _ in range(num_tasks)]
            [f.result() for f in futures]
        thread_time = time.perf_counter() - start
        results['Multi Thread'] = thread_time
        print(f"   Time: {thread_time:.2f} seconds")
        print(f"   Speedup: {single_time/thread_time:.2f}x")
        
        # Multi-process
        print("\n3. Multi-process execution...")
        start = time.perf_counter()
        with ProcessPoolExecutor(max_workers=num_tasks) as executor:
            futures = [executor.submit(self.cpu_bound_task, task_size) 
                      for _ in range(num_tasks)]
            [f.result() for f in futures]
        process_time = time.perf_counter() - start
        results['Multi Process'] = process_time
        print(f"   Time: {process_time:.2f} seconds")
        print(f"   Speedup: {single_time/process_time:.2f}x")
        
        return results
    
    def run_io_bound_comparison(self, num_requests=20):
        """Compare I/O-bound task performance across execution models."""
        print("\n" + "="*60)
        print("I/O-BOUND TASK COMPARISON")
        print("="*60)
        print(f"Task: Make {num_requests} HTTP requests")
        
        # Test URLs (using example.com for reliability)
        urls = [f"https://httpbin.org/delay/{i%3}" for i in range(num_requests)]
        
        results = {}
        
        # Single-threaded
        print("\n1. Single-threaded execution...")
        start = time.perf_counter()
        self.io_bound_task(urls)
        single_time = time.perf_counter() - start
        results['Single Thread'] = single_time
        print(f"   Time: {single_time:.2f} seconds")
        
        # Multi-threaded
        print("\n2. Multi-threaded execution...")
        start = time.perf_counter()
        
        # Split URLs among threads
        num_threads = min(10, num_requests)
        chunk_size = len(urls) // num_threads
        url_chunks = [urls[i:i+chunk_size] for i in range(0, len(urls), chunk_size)]
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(self.io_bound_task, chunk) for chunk in url_chunks]
            [f.result() for f in futures]
        
        thread_time = time.perf_counter() - start
        results['Multi Thread'] = thread_time
        print(f"   Time: {thread_time:.2f} seconds")
        print(f"   Speedup: {single_time/thread_time:.2f}x")
        
        return results
    
    def demonstrate_gil_behavior(self):
        """Show GIL behavior with visual thread timeline."""
        print("\n" + "="*60)
        print("GIL BEHAVIOR VISUALIZATION")
        print("="*60)
        
        import _thread
        
        # Track which thread is running
        thread_timeline = []
        timeline_lock = threading.Lock()
        
        def tracked_cpu_task(thread_id, iterations):
            """CPU task that tracks when it runs."""
            for i in range(iterations):
                with timeline_lock:
                    thread_timeline.append((time.perf_counter(), thread_id))
                
                # Do some CPU work
                _ = sum(j * j for j in range(1000))
        
        print("Running 3 CPU-bound threads and tracking execution...")
        
        # Clear timeline
        thread_timeline.clear()
        start_time = time.perf_counter()
        
        # Create threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=tracked_cpu_task, args=(i, 100))
            threads.append(t)
        
        # Start threads
        for t in threads:
            t.start()
        
        # Wait for completion
        for t in threads:
            t.join()
        
        # Analyze timeline
        if thread_timeline:
            # Convert to relative times
            base_time = thread_timeline[0][0]
            timeline_data = [(t - base_time, tid) for t, tid in thread_timeline]
            
            # Count thread switches
            switches = 0
            last_thread = timeline_data[0][1]
            for _, thread_id in timeline_data[1:]:
                if thread_id != last_thread:
                    switches += 1
                    last_thread = thread_id
            
            total_time = timeline_data[-1][0]
            
            print(f"\nResults:")
            print(f"  Total time: {total_time*1000:.1f} ms")
            print(f"  Thread switches: {switches}")
            print(f"  Average time between switches: {total_time*1000/switches:.1f} ms")
            
            # Show thread execution pattern
            print("\nThread execution pattern (first 50 samples):")
            print("Time (ms)  Thread")
            print("-" * 20)
            for i, (t, tid) in enumerate(timeline_data[:50]):
                print(f"{t*1000:>8.1f}  {'█' * (tid + 1)} Thread {tid}")
    
    def measure_gil_impact(self):
        """Measure GIL impact on different operations."""
        print("\n" + "="*60)
        print("GIL IMPACT MEASUREMENT")
        print("="*60)
        
        # Test different Python operations
        operations = {
            'Pure Python Math': lambda: sum(i * i for i in range(100000)),
            'List Comprehension': lambda: [i * i for i in range(100000)],
            'String Operations': lambda: ''.join(str(i) for i in range(10000)),
            'Dict Operations': lambda: {i: i*i for i in range(10000)},
        }
        
        print("\nTesting various Python operations with 4 threads:")
        print(f"{'Operation':<20} {'Single Thread':<15} {'Multi Thread':<15} {'Efficiency':<10}")
        print("-" * 60)
        
        for op_name, op_func in operations.items():
            # Single thread
            start = time.perf_counter()
            for _ in range(4):
                op_func()
            single_time = time.perf_counter() - start
            
            # Multi thread
            start = time.perf_counter()
            threads = []
            for _ in range(4):
                t = threading.Thread(target=op_func)
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
            multi_time = time.perf_counter() - start
            
            efficiency = (single_time / multi_time) * 100
            
            print(f"{op_name:<20} {single_time*1000:<15.1f} {multi_time*1000:<15.1f} {efficiency:<10.1f}%")

def visualize_results(cpu_results, io_results):
    """Create visualization of performance results."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))
    
    # CPU-bound results
    models = list(cpu_results.keys())
    times = list(cpu_results.values())
    colors = ['red', 'orange', 'green']
    
    bars1 = ax1.bar(models, times, color=colors)
    ax1.set_ylabel('Time (seconds)')
    ax1.set_title('CPU-Bound Task Performance')
    ax1.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for bar, time in zip(bars1, times):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{time:.2f}s', ha='center', va='bottom')
    
    # I/O-bound results
    models = list(io_results.keys())
    times = list(io_results.values())
    colors = ['red', 'green']
    
    bars2 = ax2.bar(models, times, color=colors)
    ax2.set_ylabel('Time (seconds)')
    ax2.set_title('I/O-Bound Task Performance')
    ax2.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for bar, time in zip(bars2, times):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{time:.2f}s', ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig('gil_performance_comparison.png', dpi=150)
    print("\n✓ Performance comparison chart saved as 'gil_performance_comparison.png'")

def main():
    """Run GIL demonstrations."""
    print("Python GIL (Global Interpreter Lock) Demonstration")
    print(f"Python version: {sys.version}")
    print(f"CPU count: {multiprocessing.cpu_count()}")
    
    demonstrator = GILDemonstrator()
    
    try:
        # 1. Compare CPU-bound performance
        cpu_results = demonstrator.run_cpu_bound_comparison()
        
        # 2. Compare I/O-bound performance
        io_results = demonstrator.run_io_bound_comparison()
        
        # 3. Demonstrate GIL behavior
        demonstrator.demonstrate_gil_behavior()
        
        # 4. Measure GIL impact
        demonstrator.measure_gil_impact()
        
        # 5. Visualize results
        print("\nGenerating performance visualization...")
        visualize_results(cpu_results, io_results)
        
        # Summary
        print("\n" + "="*60)
        print("KEY INSIGHTS")
        print("="*60)
        print("1. CPU-bound tasks show no speedup with threading due to GIL")
        print("2. I/O-bound tasks benefit significantly from threading")
        print("3. Multiprocessing provides true parallelism for CPU-bound tasks")
        print("4. The GIL switches between threads approximately every 5ms")
        print("5. Threading is ideal for I/O-waiting, not CPU computation")
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
