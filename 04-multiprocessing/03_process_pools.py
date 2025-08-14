#!/usr/bin/env python3
"""
Process Pool Patterns: Efficient parallel processing

This script demonstrates various process pool patterns and best practices
for CPU-bound parallel processing in Python.
"""

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import random
import math
import os
import sys
import psutil
from functools import partial
import numpy as np
from datetime import datetime
import pickle

class ProcessPoolDemo:
    """Demonstrates various process pool patterns."""
    
    def __init__(self):
        self.cpu_count = mp.cpu_count()
        print(f"System has {self.cpu_count} CPU cores")
    
    @staticmethod
    def cpu_bound_task(x):
        """CPU-intensive task for demonstration."""
        # Calculate prime factors
        factors = []
        n = x
        
        # Check for 2
        while n % 2 == 0:
            factors.append(2)
            n = n // 2
        
        # Check odd numbers
        for i in range(3, int(math.sqrt(n)) + 1, 2):
            while n % i == 0:
                factors.append(i)
                n = n // i
        
        if n > 2:
            factors.append(n)
        
        # Simulate more work
        result = sum(factors) * len(factors)
        time.sleep(0.01)  # Small delay to simulate I/O
        
        return {
            'input': x,
            'factors': factors,
            'result': result,
            'process_id': os.getpid()
        }
    
    def demonstrate_pool_methods(self):
        """Demonstrate different Pool methods."""
        print(f"\n{'='*60}")
        print("POOL METHODS DEMONSTRATION")
        print(f"{'='*60}")
        
        # Test data
        numbers = [random.randint(10000, 100000) for _ in range(20)]
        
        with mp.Pool(processes=4) as pool:
            # 1. map - Blocking, ordered results
            print("\n1. pool.map() - Blocking, preserves order:")
            start = time.perf_counter()
            results = pool.map(self.cpu_bound_task, numbers[:5])
            elapsed = time.perf_counter() - start
            
            print(f"   Processed {len(results)} items in {elapsed:.2f}s")
            for r in results[:2]:
                print(f"   Input: {r['input']}, Result: {r['result']}, PID: {r['process_id']}")
            
            # 2. map_async - Non-blocking
            print("\n2. pool.map_async() - Non-blocking:")
            start = time.perf_counter()
            async_result = pool.map_async(self.cpu_bound_task, numbers[5:10])
            
            # Do other work while processing
            print("   Doing other work while pool processes...")
            time.sleep(0.1)
            
            # Get results
            results = async_result.get()
            elapsed = time.perf_counter() - start
            print(f"   Processed {len(results)} items in {elapsed:.2f}s")
            
            # 3. imap - Lazy iterator
            print("\n3. pool.imap() - Lazy iterator, preserves order:")
            start = time.perf_counter()
            
            for i, result in enumerate(pool.imap(self.cpu_bound_task, numbers[10:15])):
                print(f"   Received result {i+1}: Input={result['input']}, Result={result['result']}")
                if i >= 2:  # Just show first 3
                    break
            
            # Consume rest
            list(pool.imap(self.cpu_bound_task, numbers[10:15]))
            elapsed = time.perf_counter() - start
            print(f"   Total time: {elapsed:.2f}s")
            
            # 4. imap_unordered - Lazy iterator, any order
            print("\n4. pool.imap_unordered() - Results as they complete:")
            start = time.perf_counter()
            
            for i, result in enumerate(pool.imap_unordered(self.cpu_bound_task, numbers[15:20])):
                print(f"   Received: Input={result['input']}, PID={result['process_id']}")
                if i >= 2:  # Just show first 3
                    break
            
            # Consume rest
            list(pool.imap_unordered(self.cpu_bound_task, numbers[15:20]))
            elapsed = time.perf_counter() - start
            print(f"   Total time: {elapsed:.2f}s")
            
            # 5. apply - Single task (not parallel)
            print("\n5. pool.apply() - Single task, blocking:")
            start = time.perf_counter()
            result = pool.apply(self.cpu_bound_task, (12345,))
            elapsed = time.perf_counter() - start
            print(f"   Result: {result['result']}, Time: {elapsed:.2f}s")
            
            # 6. apply_async - Single task, non-blocking
            print("\n6. pool.apply_async() - Single task, non-blocking:")
            future = pool.apply_async(self.cpu_bound_task, (54321,))
            print("   Task submitted, doing other work...")
            result = future.get()
            print(f"   Result: {result['result']}")
    
    def demonstrate_concurrent_futures(self):
        """Demonstrate ProcessPoolExecutor patterns."""
        print(f"\n{'='*60}")
        print("CONCURRENT.FUTURES PROCESSPOOLEXECUTOR")
        print(f"{'='*60}")
        
        # Test data
        tasks = [(i, random.randint(10000, 100000)) for i in range(10)]
        
        with ProcessPoolExecutor(max_workers=4) as executor:
            # 1. Submit individual tasks
            print("\n1. Submitting individual tasks:")
            futures = {}
            
            for task_id, number in tasks[:5]:
                future = executor.submit(self.cpu_bound_task, number)
                futures[future] = task_id
            
            # Process as completed
            print("   Processing results as they complete:")
            for future in as_completed(futures):
                task_id = futures[future]
                try:
                    result = future.result()
                    print(f"   Task {task_id} completed: Result={result['result']}")
                except Exception as e:
                    print(f"   Task {task_id} failed: {e}")
            
            # 2. Map pattern
            print("\n2. Using executor.map():")
            numbers = [n for _, n in tasks[5:]]
            results = list(executor.map(self.cpu_bound_task, numbers))
            print(f"   Processed {len(results)} items")
            
            # 3. Batch submission with timeout
            print("\n3. Batch submission with timeout handling:")
            futures = []
            
            for _, number in tasks:
                future = executor.submit(self.cpu_bound_task, number)
                futures.append(future)
            
            # Wait with timeout
            completed = 0
            for future in futures:
                try:
                    result = future.result(timeout=1.0)
                    completed += 1
                except TimeoutError:
                    print("   Task timed out")
            
            print(f"   Completed {completed}/{len(futures)} tasks")
    
    def demonstrate_chunking(self):
        """Demonstrate efficient chunking for large datasets."""
        print(f"\n{'='*60}")
        print("CHUNKING FOR EFFICIENT PROCESSING")
        print(f"{'='*60}")
        
        # Large dataset
        data_size = 10000
        data = list(range(data_size))
        
        def process_chunk(chunk):
            """Process a chunk of data."""
            return {
                'chunk_size': len(chunk),
                'sum': sum(chunk),
                'mean': sum(chunk) / len(chunk),
                'process_id': os.getpid()
            }
        
        # Compare different chunk sizes
        chunk_sizes = [1, 10, 100, 1000]
        
        print(f"\nProcessing {data_size} items with different chunk sizes:")
        print(f"{'Chunk Size':<12} {'Time (s)':<10} {'Chunks':<10} {'Efficiency':<10}")
        print("-" * 45)
        
        with mp.Pool(processes=4) as pool:
            for chunk_size in chunk_sizes:
                # Create chunks
                chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
                
                # Process
                start = time.perf_counter()
                results = pool.map(process_chunk, chunks)
                elapsed = time.perf_counter() - start
                
                # Calculate efficiency (items/second)
                efficiency = data_size / elapsed
                
                print(f"{chunk_size:<12} {elapsed:<10.3f} {len(chunks):<10} {efficiency:<10.0f}")
        
        print("\n✓ Larger chunks reduce overhead but may reduce parallelism")
        print("✓ Optimal chunk size depends on task complexity")
    
    def demonstrate_initializer(self):
        """Demonstrate pool initializer for expensive setup."""
        print(f"\n{'='*60}")
        print("POOL INITIALIZER PATTERN")
        print(f"{'='*60}")
        
        # Global variable for worker state
        worker_state = None
        
        def expensive_initialization(config):
            """Initialize expensive resources once per worker."""
            global worker_state
            
            print(f"  Worker {os.getpid()}: Initializing with config {config}")
            
            # Simulate expensive setup
            time.sleep(0.5)
            
            # Store in global state
            worker_state = {
                'config': config,
                'initialized_at': datetime.now(),
                'process_id': os.getpid(),
                'resources': list(range(1000))  # Simulate loaded resources
            }
        
        def task_using_state(x):
            """Task that uses pre-initialized state."""
            global worker_state
            
            if worker_state is None:
                return {'error': 'Worker not initialized'}
            
            # Use the pre-initialized resources
            result = sum(worker_state['resources'][:x])
            
            return {
                'input': x,
                'result': result,
                'worker_pid': worker_state['process_id'],
                'config': worker_state['config']
            }
        
        # Configuration for workers
        config = {'mode': 'production', 'cache_size': 1000}
        
        print("\nStarting pool with initializer...")
        
        # Create pool with initializer
        with mp.Pool(
            processes=3,
            initializer=expensive_initialization,
            initargs=(config,)
        ) as pool:
            
            # Submit tasks
            inputs = [10, 20, 30, 40, 50]
            results = pool.map(task_using_state, inputs)
            
            print("\nResults:")
            for r in results:
                if 'error' not in r:
                    print(f"  Input: {r['input']}, Result: {r['result']}, "
                          f"Worker: {r['worker_pid']}, Config: {r['config']['mode']}")
        
        print("\n✓ Initializer runs once per worker process")
        print("✓ Useful for loading models, connecting to databases, etc.")
    
    def demonstrate_error_handling(self):
        """Demonstrate error handling in process pools."""
        print(f"\n{'='*60}")
        print("ERROR HANDLING IN PROCESS POOLS")
        print(f"{'='*60}")
        
        def risky_task(x):
            """Task that might fail."""
            if x < 0:
                raise ValueError(f"Negative input not allowed: {x}")
            elif x == 0:
                raise ZeroDivisionError("Cannot divide by zero")
            elif x > 100:
                # Simulate crash
                os._exit(1)
            
            return x ** 2
        
        # Test data with some problematic values
        test_data = [5, -3, 10, 0, 20, 150, 30]
        
        # 1. Using Pool with error handling
        print("\n1. Pool.map with error handling:")
        
        def safe_wrapper(x):
            """Wrapper that catches exceptions."""
            try:
                return ('success', risky_task(x))
            except Exception as e:
                return ('error', str(e))
        
        with mp.Pool(processes=2) as pool:
            results = pool.map(safe_wrapper, test_data)
            
            for i, (status, result) in enumerate(results):
                if status == 'success':
                    print(f"  Task {i}: Success - {result}")
                else:
                    print(f"  Task {i}: Failed - {result}")
        
        # 2. Using ProcessPoolExecutor
        print("\n2. ProcessPoolExecutor with individual error handling:")
        
        with ProcessPoolExecutor(max_workers=2) as executor:
            # Submit all tasks
            futures = {executor.submit(risky_task, x): x for x in test_data}
            
            # Process results
            for future in as_completed(futures):
                x = futures[future]
                try:
                    result = future.result()
                    print(f"  Input {x}: Success - {result}")
                except ValueError as e:
                    print(f"  Input {x}: ValueError - {e}")
                except ZeroDivisionError as e:
                    print(f"  Input {x}: ZeroDivisionError - {e}")
                except Exception as e:
                    print(f"  Input {x}: Unexpected error - {e}")
    
    def demonstrate_memory_efficiency(self):
        """Demonstrate memory-efficient processing of large data."""
        print(f"\n{'='*60}")
        print("MEMORY-EFFICIENT PROCESSING")
        print(f"{'='*60}")
        
        def process_data_item(item):
            """Process a single data item."""
            # Simulate processing
            result = sum(item) / len(item)
            return result
        
        def data_generator(n_items, item_size):
            """Generate data items on demand."""
            for i in range(n_items):
                # Generate item on-the-fly
                yield [random.random() for _ in range(item_size)]
        
        # Parameters
        n_items = 1000
        item_size = 1000
        
        print(f"\nProcessing {n_items} items of size {item_size} each")
        print("Using generator to avoid loading all data into memory")
        
        # Monitor memory usage
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        with mp.Pool(processes=4) as pool:
            # Process using imap with small chunksize
            results = []
            
            start = time.perf_counter()
            for i, result in enumerate(pool.imap(
                process_data_item,
                data_generator(n_items, item_size),
                chunksize=10
            )):
                results.append(result)
                
                # Show progress
                if (i + 1) % 100 == 0:
                    current_memory = process.memory_info().rss / 1024 / 1024
                    print(f"  Processed {i+1} items, Memory: {current_memory:.1f} MB "
                          f"(+{current_memory - initial_memory:.1f} MB)")
            
            elapsed = time.perf_counter() - start
        
        final_memory = process.memory_info().rss / 1024 / 1024
        
        print(f"\nCompleted in {elapsed:.2f} seconds")
        print(f"Memory usage: {initial_memory:.1f} MB -> {final_memory:.1f} MB "
              f"(+{final_memory - initial_memory:.1f} MB)")
        print(f"Average result: {sum(results) / len(results):.4f}")
        
        print("\n✓ Generators + imap enable processing of large datasets")
        print("✓ Small chunksize reduces memory usage at cost of more overhead")

def demonstrate_map_reduce_pattern():
    """Demonstrate the map-reduce pattern with process pools."""
    print(f"\n{'='*60}")
    print("MAP-REDUCE PATTERN")
    print(f"{'='*60}")
    
    # Example: Word count across multiple documents
    documents = [
        "The quick brown fox jumps over the lazy dog",
        "Python multiprocessing enables true parallel processing",
        "Process pools efficiently manage worker processes",
        "The map reduce pattern is useful for data processing",
        "Python is great for concurrent and parallel programming"
    ] * 20  # Replicate for more data
    
    def map_word_count(document):
        """Map function: count words in a document."""
        word_count = {}
        for word in document.lower().split():
            word_count[word] = word_count.get(word, 0) + 1
        return word_count
    
    def reduce_word_counts(counts1, counts2):
        """Reduce function: merge two word count dictionaries."""
        result = counts1.copy()
        for word, count in counts2.items():
            result[word] = result.get(word, 0) + count
        return result
    
    print(f"Processing {len(documents)} documents...")
    
    with mp.Pool() as pool:
        # Map phase
        start = time.perf_counter()
        mapped_results = pool.map(map_word_count, documents)
        map_time = time.perf_counter() - start
        
        # Reduce phase (sequential for simplicity)
        start = time.perf_counter()
        final_result = {}
        for word_count in mapped_results:
            final_result = reduce_word_counts(final_result, word_count)
        reduce_time = time.perf_counter() - start
    
    # Show results
    total_words = sum(final_result.values())
    unique_words = len(final_result)
    top_words = sorted(final_result.items(), key=lambda x: x[1], reverse=True)[:5]
    
    print(f"\nResults:")
    print(f"  Map phase: {map_time:.3f} seconds")
    print(f"  Reduce phase: {reduce_time:.3f} seconds")
    print(f"  Total words: {total_words}")
    print(f"  Unique words: {unique_words}")
    print(f"  Top 5 words: {top_words}")

def main():
    """Run process pool demonstrations."""
    print("Process Pool Patterns and Best Practices")
    print("=" * 60)
    
    demo = ProcessPoolDemo()
    
    try:
        # 1. Pool methods
        demo.demonstrate_pool_methods()
        
        # 2. Concurrent futures
        demo.demonstrate_concurrent_futures()
        
        # 3. Chunking strategies
        demo.demonstrate_chunking()
        
        # 4. Initializer pattern
        demo.demonstrate_initializer()
        
        # 5. Error handling
        demo.demonstrate_error_handling()
        
        # 6. Memory efficiency
        demo.demonstrate_memory_efficiency()
        
        # 7. Map-reduce pattern
        demonstrate_map_reduce_pattern()
        
        # Summary
        print(f"\n{'='*60}")
        print("PROCESS POOL BEST PRACTICES")
        print(f"{'='*60}")
        print("\n1. Use Pool.map() for simple parallel mapping")
        print("2. Use imap/imap_unordered for large datasets")
        print("3. Choose appropriate chunk size for efficiency")
        print("4. Use initializer for expensive per-worker setup")
        print("5. Handle errors gracefully with try-except")
        print("6. Use generators for memory-efficient processing")
        print("7. Consider ProcessPoolExecutor for more control")
        print("8. Always use context managers or ensure cleanup")
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    mp.freeze_support()
    main()
