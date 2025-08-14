#!/usr/bin/env python3
"""
Thread Coordination Patterns

This script demonstrates advanced coordination patterns including
producer-consumer, pipeline, work stealing, and map-reduce.
"""

import threading
import queue
import time
import random
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import heapq

class WorkStealingQueue:
    """Thread-safe work-stealing queue."""
    
    def __init__(self):
        self.deque = deque()
        self.lock = threading.Lock()
    
    def push(self, item):
        """Add item to the top."""
        with self.lock:
            self.deque.append(item)
    
    def pop(self):
        """Remove item from the top (owner thread)."""
        with self.lock:
            if self.deque:
                return self.deque.pop()
            return None
    
    def steal(self):
        """Remove item from the bottom (thief thread)."""
        with self.lock:
            if self.deque:
                return self.deque.popleft()
            return None
    
    def size(self):
        """Get queue size."""
        with self.lock:
            return len(self.deque)

class PipelineStage:
    """A stage in a processing pipeline."""
    
    def __init__(self, name, process_func, num_workers=1):
        self.name = name
        self.process_func = process_func
        self.num_workers = num_workers
        self.input_queue = queue.Queue(maxsize=100)
        self.output_queue = None
        self.workers = []
        self.running = False
        self.stats = defaultdict(int)
    
    def set_output(self, output_queue):
        """Set the output queue for this stage."""
        self.output_queue = output_queue
    
    def start(self):
        """Start worker threads."""
        self.running = True
        for i in range(self.num_workers):
            worker = threading.Thread(
                target=self._worker,
                name=f"{self.name}-Worker-{i}"
            )
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
    
    def stop(self):
        """Stop all workers."""
        self.running = False
        # Send stop signals
        for _ in self.workers:
            self.input_queue.put(None)
        # Wait for workers
        for worker in self.workers:
            worker.join()
    
    def _worker(self):
        """Worker thread function."""
        while self.running:
            try:
                item = self.input_queue.get(timeout=1)
                if item is None:
                    break
                
                # Process item
                start = time.time()
                result = self.process_func(item)
                duration = time.time() - start
                
                # Update stats
                self.stats['processed'] += 1
                self.stats['total_time'] += duration
                
                # Send to next stage
                if self.output_queue and result is not None:
                    self.output_queue.put(result)
                
            except queue.Empty:
                continue
            except Exception as e:
                self.stats['errors'] += 1
                print(f"Error in {self.name}: {e}")

def demonstrate_producer_consumer():
    """Demonstrate producer-consumer pattern."""
    print(f"\n{'='*60}")
    print("PRODUCER-CONSUMER PATTERN")
    print(f"{'='*60}")
    
    # Shared queue with bounded capacity
    buffer = queue.Queue(maxsize=10)
    produced = []
    consumed = []
    
    def producer(producer_id, count):
        """Producer function."""
        for i in range(count):
            item = f"P{producer_id}-Item{i}"
            buffer.put(item)  # Blocks if full
            produced.append((producer_id, item))
            print(f"  Producer {producer_id}: Produced {item}")
            time.sleep(random.uniform(0.01, 0.05))
    
    def consumer(consumer_id):
        """Consumer function."""
        while True:
            try:
                item = buffer.get(timeout=0.5)
                consumed.append((consumer_id, item))
                print(f"  Consumer {consumer_id}: Consumed {item}")
                time.sleep(random.uniform(0.02, 0.08))
                buffer.task_done()
            except queue.Empty:
                break
    
    # Start producers
    producers = []
    for i in range(3):
        t = threading.Thread(target=producer, args=(i, 5))
        t.start()
        producers.append(t)
    
    # Start consumers
    consumers = []
    for i in range(2):
        t = threading.Thread(target=consumer, args=(i,))
        t.start()
        consumers.append(t)
    
    # Wait for producers
    for t in producers:
        t.join()
    
    # Wait for queue to empty
    buffer.join()
    
    # Stop consumers
    for t in consumers:
        t.join()
    
    print(f"\nResults:")
    print(f"  Items produced: {len(produced)}")
    print(f"  Items consumed: {len(consumed)}")
    
    # Show distribution
    consumer_counts = defaultdict(int)
    for consumer_id, _ in consumed:
        consumer_counts[consumer_id] += 1
    
    print(f"  Consumer distribution: {dict(consumer_counts)}")

def demonstrate_pipeline():
    """Demonstrate pipeline pattern."""
    print(f"\n{'='*60}")
    print("PIPELINE PATTERN")
    print(f"{'='*60}")
    
    # Define pipeline stages
    def stage1_fetch(item):
        """Fetch data."""
        time.sleep(0.02)
        return {'id': item, 'data': f"raw_data_{item}"}
    
    def stage2_process(item):
        """Process data."""
        time.sleep(0.03)
        item['processed'] = item['data'].upper()
        return item
    
    def stage3_enrich(item):
        """Enrich data."""
        time.sleep(0.01)
        item['timestamp'] = datetime.now()
        item['enriched'] = True
        return item
    
    # Create pipeline
    stages = [
        PipelineStage("Fetch", stage1_fetch, num_workers=2),
        PipelineStage("Process", stage2_process, num_workers=3),
        PipelineStage("Enrich", stage3_enrich, num_workers=2)
    ]
    
    # Connect stages
    for i in range(len(stages) - 1):
        stages[i].set_output(stages[i + 1].input_queue)
    
    # Final output queue
    output_queue = queue.Queue()
    stages[-1].set_output(output_queue)
    
    # Start pipeline
    print("Starting pipeline...")
    for stage in stages:
        stage.start()
    
    # Feed input
    start_time = time.time()
    input_count = 20
    
    for i in range(input_count):
        stages[0].input_queue.put(i)
    
    # Collect output
    results = []
    for _ in range(input_count):
        result = output_queue.get()
        results.append(result)
    
    elapsed = time.time() - start_time
    
    # Stop pipeline
    for stage in stages:
        stage.stop()
    
    # Show statistics
    print(f"\nPipeline statistics:")
    print(f"  Total time: {elapsed:.2f}s")
    print(f"  Throughput: {input_count/elapsed:.1f} items/second")
    
    for stage in stages:
        stats = stage.stats
        if stats['processed'] > 0:
            avg_time = stats['total_time'] / stats['processed']
            print(f"  {stage.name}: {stats['processed']} items, "
                  f"avg {avg_time*1000:.1f}ms/item")

def demonstrate_work_stealing():
    """Demonstrate work-stealing pattern."""
    print(f"\n{'='*60}")
    print("WORK-STEALING PATTERN")
    print(f"{'='*60}")
    
    num_threads = 4
    queues = [WorkStealingQueue() for _ in range(num_threads)]
    results = [[] for _ in range(num_threads)]
    steal_counts = [0] * num_threads
    
    def worker(worker_id):
        """Worker with work stealing."""
        my_queue = queues[worker_id]
        my_results = results[worker_id]
        
        while True:
            # Try to get work from own queue
            item = my_queue.pop()
            
            if item is None:
                # Try to steal from others
                stolen = False
                for i in range(num_threads):
                    if i != worker_id:
                        item = queues[i].steal()
                        if item:
                            steal_counts[worker_id] += 1
                            stolen = True
                            print(f"  Worker {worker_id} stole from Worker {i}: {item}")
                            break
                
                if not stolen:
                    break  # No work anywhere
            
            # Process item
            if item:
                time.sleep(0.01)  # Simulate work
                my_results.append(item)
    
    # Distribute work unevenly
    print("Distributing work (unevenly)...")
    total_work = 40
    distribution = [20, 10, 5, 5]  # Uneven distribution
    
    for i in range(num_threads):
        for j in range(distribution[i]):
            queues[i].push(f"Task-{i}-{j}")
    
    # Show initial distribution
    print(f"Initial distribution: {[q.size() for q in queues]}")
    
    # Start workers
    threads = []
    start_time = time.time()
    
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)
    
    # Wait for completion
    for t in threads:
        t.join()
    
    elapsed = time.time() - start_time
    
    # Show results
    print(f"\nWork-stealing results:")
    print(f"  Total time: {elapsed:.2f}s")
    
    for i in range(num_threads):
        print(f"  Worker {i}: Processed {len(results[i])} items, "
              f"Stole {steal_counts[i]} items")
    
    print(f"  Total steals: {sum(steal_counts)}")
    print("  ✓ Work stealing balanced the load automatically")

def demonstrate_map_reduce():
    """Demonstrate map-reduce pattern with threads."""
    print(f"\n{'='*60}")
    print("MAP-REDUCE PATTERN")
    print(f"{'='*60}")
    
    # Sample data: word counting
    documents = [
        "parallel programming with threads",
        "thread synchronization patterns",
        "advanced thread management",
        "concurrent programming patterns",
        "thread pool optimization"
    ] * 4  # Replicate for more data
    
    def map_function(doc):
        """Map: document -> word counts."""
        word_counts = defaultdict(int)
        for word in doc.lower().split():
            word_counts[word] += 1
        return dict(word_counts)
    
    def reduce_function(counts1, counts2):
        """Reduce: merge two word count dicts."""
        result = counts1.copy()
        for word, count in counts2.items():
            result[word] = result.get(word, 0) + count
        return result
    
    print(f"Processing {len(documents)} documents...")
    
    # Map phase
    with ThreadPoolExecutor(max_workers=4) as executor:
        start_time = time.time()
        
        # Map
        mapped = list(executor.map(map_function, documents))
        map_time = time.time() - start_time
        
        # Reduce in parallel (tree reduction)
        while len(mapped) > 1:
            # Pair up results
            pairs = []
            for i in range(0, len(mapped), 2):
                if i + 1 < len(mapped):
                    pairs.append((mapped[i], mapped[i + 1]))
                else:
                    pairs.append((mapped[i], {}))
            
            # Reduce pairs in parallel
            mapped = list(executor.map(lambda p: reduce_function(p[0], p[1]), pairs))
        
        reduce_time = time.time() - start_time - map_time
    
    final_result = mapped[0]
    
    # Show results
    print(f"\nMap-Reduce results:")
    print(f"  Map time: {map_time:.3f}s")
    print(f"  Reduce time: {reduce_time:.3f}s")
    print(f"  Total unique words: {len(final_result)}")
    
    # Top words
    top_words = sorted(final_result.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"  Top 5 words: {top_words}")

def demonstrate_fork_join():
    """Demonstrate fork-join pattern."""
    print(f"\n{'='*60}")
    print("FORK-JOIN PATTERN")
    print(f"{'='*60}")
    
    class ForkJoinTask:
        """Recursive fork-join task."""
        
        def __init__(self, data, threshold=10):
            self.data = data
            self.threshold = threshold
            self.result = None
        
        def compute(self):
            """Compute the task, forking if needed."""
            if len(self.data) <= self.threshold:
                # Base case: process directly
                self.result = sum(x * x for x in self.data)
                return self.result
            else:
                # Fork: split into subtasks
                mid = len(self.data) // 2
                left_task = ForkJoinTask(self.data[:mid], self.threshold)
                right_task = ForkJoinTask(self.data[mid:], self.threshold)
                
                # Execute subtasks in parallel
                with ThreadPoolExecutor(max_workers=2) as executor:
                    left_future = executor.submit(left_task.compute)
                    right_future = executor.submit(right_task.compute)
                    
                    left_result = left_future.result()
                    right_result = right_future.result()
                
                # Join: combine results
                self.result = left_result + right_result
                return self.result
    
    # Test data
    data = list(range(1000))
    
    print(f"Computing sum of squares for {len(data)} numbers...")
    
    # Sequential computation
    start = time.time()
    sequential_result = sum(x * x for x in data)
    sequential_time = time.time() - start
    
    # Fork-join computation
    start = time.time()
    task = ForkJoinTask(data, threshold=50)
    parallel_result = task.compute()
    parallel_time = time.time() - start
    
    print(f"\nResults:")
    print(f"  Sequential: {sequential_result} in {sequential_time*1000:.1f}ms")
    print(f"  Fork-Join: {parallel_result} in {parallel_time*1000:.1f}ms")
    print(f"  Speedup: {sequential_time/parallel_time:.2f}x")
    print(f"  Results match: {sequential_result == parallel_result}")

def main():
    """Run all coordination pattern demonstrations."""
    print("Thread Coordination Patterns")
    print("=" * 60)
    
    try:
        demonstrate_producer_consumer()
        demonstrate_pipeline()
        demonstrate_work_stealing()
        demonstrate_map_reduce()
        demonstrate_fork_join()
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("COORDINATION PATTERNS SUMMARY")
    print(f"{'='*60}")
    print("1. Producer-Consumer: Decouples data generation from processing")
    print("2. Pipeline: Stages process data in sequence")
    print("3. Work Stealing: Automatic load balancing")
    print("4. Map-Reduce: Parallel data processing")
    print("5. Fork-Join: Recursive parallel decomposition")

if __name__ == "__main__":
    main()
