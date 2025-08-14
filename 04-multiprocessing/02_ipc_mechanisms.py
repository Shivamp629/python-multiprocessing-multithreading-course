#!/usr/bin/env python3
"""
Inter-Process Communication (IPC) Mechanisms

This script demonstrates various IPC methods available in Python's
multiprocessing module for communication between processes.
"""

import multiprocessing as mp
import time
import queue
import random
import pickle
import sys
import os
from datetime import datetime
import numpy as np
from multiprocessing import shared_memory

class IPCDemo:
    """Demonstrates various IPC mechanisms."""
    
    def demonstrate_pipes(self):
        """Demonstrate communication using Pipes."""
        print(f"\n{'='*60}")
        print("PIPE COMMUNICATION")
        print(f"{'='*60}")
        
        # Create a pipe
        parent_conn, child_conn = mp.Pipe()
        
        def sender(conn, messages):
            """Process that sends messages through pipe."""
            pid = os.getpid()
            for i, msg in enumerate(messages):
                print(f"  Sender (PID {pid}): Sending '{msg}'")
                conn.send(msg)
                time.sleep(0.1)
            
            # Send sentinel to indicate completion
            conn.send(None)
            conn.close()
        
        def receiver(conn):
            """Process that receives messages from pipe."""
            pid = os.getpid()
            received = []
            
            while True:
                try:
                    msg = conn.recv()
                    if msg is None:
                        break
                    print(f"  Receiver (PID {pid}): Received '{msg}'")
                    received.append(msg)
                except EOFError:
                    break
            
            conn.close()
            return received
        
        # Test data
        messages = ["Hello", "World", {"type": "dict", "value": 42}, [1, 2, 3]]
        
        # Create processes
        sender_proc = mp.Process(target=sender, args=(child_conn, messages))
        
        # Start sender
        sender_proc.start()
        
        # Receive in main process
        print("\nSending messages through pipe:")
        received = receiver(parent_conn)
        
        # Wait for sender to finish
        sender_proc.join()
        
        print(f"\nSummary: Sent {len(messages)} messages, received {len(received)}")
        print("✓ Pipes allow bidirectional communication between two processes")
        
        # Demonstrate duplex pipe
        print("\n" + "-"*40)
        print("Duplex (bidirectional) pipe:")
        
        def echo_server(conn):
            """Echo server that sends back modified messages."""
            while True:
                try:
                    msg = conn.recv()
                    if msg == 'STOP':
                        break
                    response = f"ECHO: {msg}"
                    conn.send(response)
                except EOFError:
                    break
            conn.close()
        
        # Create duplex pipe
        server_conn, client_conn = mp.Pipe(duplex=True)
        
        # Start echo server
        server_proc = mp.Process(target=echo_server, args=(server_conn,))
        server_proc.start()
        
        # Client communication
        for msg in ["Hello", "Testing", "123"]:
            client_conn.send(msg)
            response = client_conn.recv()
            print(f"  Sent: '{msg}', Received: '{response}'")
        
        client_conn.send('STOP')
        server_proc.join()
        print("✓ Duplex pipes enable request-response patterns")
    
    def demonstrate_queues(self):
        """Demonstrate Queue-based communication."""
        print(f"\n{'='*60}")
        print("QUEUE COMMUNICATION")
        print(f"{'='*60}")
        
        # Create different types of queues
        simple_queue = mp.Queue()
        
        def producer(queue, name, items):
            """Producer that adds items to queue."""
            pid = os.getpid()
            for item in items:
                print(f"  Producer {name} (PID {pid}): Producing {item}")
                queue.put(item)
                time.sleep(random.uniform(0.1, 0.3))
            
            # Send sentinel
            queue.put(None)
        
        def consumer(queue, name):
            """Consumer that processes items from queue."""
            pid = os.getpid()
            consumed = []
            
            while True:
                try:
                    item = queue.get(timeout=2)
                    if item is None:
                        # Put it back for other consumers
                        queue.put(None)
                        break
                    
                    print(f"  Consumer {name} (PID {pid}): Consumed {item}")
                    consumed.append(item)
                    time.sleep(random.uniform(0.1, 0.2))
                    
                except queue.Empty:
                    print(f"  Consumer {name}: Queue empty, timeout")
                    break
            
            return consumed
        
        # Test with multiple producers and consumers
        print("\n1. Multiple producers, multiple consumers:")
        
        # Start producers
        producers = []
        for i in range(2):
            items = [f"P{i}-Item{j}" for j in range(3)]
            p = mp.Process(target=producer, args=(simple_queue, f"P{i}", items))
            p.start()
            producers.append(p)
        
        # Start consumers
        consumers = []
        for i in range(3):
            p = mp.Process(target=consumer, args=(simple_queue, f"C{i}"))
            p.start()
            consumers.append(p)
        
        # Wait for completion
        for p in producers:
            p.join()
        
        for p in consumers:
            p.join()
        
        print("\n✓ Queues handle multiple producers/consumers automatically")
        
        # Demonstrate priority queue
        print("\n2. Priority Queue demonstration:")
        
        def priority_worker(priority_queue):
            """Worker that processes items by priority."""
            while True:
                try:
                    priority, item = priority_queue.get(timeout=1)
                    if item is None:
                        break
                    print(f"  Processing (priority {priority}): {item}")
                    time.sleep(0.2)
                except queue.Empty:
                    break
        
        # Create priority queue (using manager for cross-process)
        manager = mp.Manager()
        priority_queue = manager.Queue()
        
        # Add items with different priorities
        items = [
            (3, "Low priority task"),
            (1, "High priority task"),
            (2, "Medium priority task"),
            (1, "Another high priority"),
            (3, "Another low priority")
        ]
        
        # Sort by priority before putting in queue
        for priority, item in sorted(items):
            priority_queue.put((priority, item))
        
        priority_queue.put((0, None))  # Sentinel
        
        # Process items
        worker = mp.Process(target=priority_worker, args=(priority_queue,))
        worker.start()
        worker.join()
        
        print("✓ Priority queues process items in priority order")
    
    def demonstrate_shared_memory(self):
        """Demonstrate shared memory mechanisms."""
        print(f"\n{'='*60}")
        print("SHARED MEMORY")
        print(f"{'='*60}")
        
        # 1. Value and Array
        print("\n1. Shared Value and Array:")
        
        # Shared value
        shared_counter = mp.Value('i', 0)  # 'i' for integer
        shared_float = mp.Value('d', 0.0)  # 'd' for double
        
        # Shared array
        shared_array = mp.Array('i', 10)  # Array of 10 integers
        
        def increment_shared(counter, float_val, array, lock):
            """Increment shared values."""
            pid = os.getpid()
            
            for i in range(100):
                with lock:
                    counter.value += 1
                    float_val.value += 0.1
                    array[i % 10] += 1
            
            print(f"  Process {pid}: Incremented values")
        
        lock = mp.Lock()
        
        # Create processes
        processes = []
        for i in range(3):
            p = mp.Process(target=increment_shared, 
                          args=(shared_counter, shared_float, shared_array, lock))
            p.start()
            processes.append(p)
        
        for p in processes:
            p.join()
        
        print(f"  Final counter: {shared_counter.value}")
        print(f"  Final float: {shared_float.value:.1f}")
        print(f"  Final array: {list(shared_array)}")
        print("✓ Value and Array provide simple shared memory")
        
        # 2. Shared Memory (Python 3.8+)
        if sys.version_info >= (3, 8):
            print("\n2. SharedMemory for NumPy arrays:")
            
            def create_shared_numpy_array(shape, dtype=np.float64):
                """Create a numpy array backed by shared memory."""
                size = np.prod(shape) * np.dtype(dtype).itemsize
                shm = shared_memory.SharedMemory(create=True, size=size)
                array = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
                return shm, array
            
            def worker_numpy(shm_name, shape, worker_id):
                """Worker that modifies shared numpy array."""
                # Attach to existing shared memory
                existing_shm = shared_memory.SharedMemory(name=shm_name)
                array = np.ndarray(shape, dtype=np.float64, buffer=existing_shm.buf)
                
                # Modify array
                array[worker_id] = np.random.rand(shape[1])
                print(f"  Worker {worker_id}: Modified row {worker_id}")
                
                # Clean up
                existing_shm.close()
            
            # Create shared array
            shape = (4, 100)
            shm, shared_np_array = create_shared_numpy_array(shape)
            shared_np_array.fill(0)
            
            print(f"  Created shared array: shape={shape}")
            
            # Start workers
            processes = []
            for i in range(4):
                p = mp.Process(target=worker_numpy, args=(shm.name, shape, i))
                p.start()
                processes.append(p)
            
            for p in processes:
                p.join()
            
            # Check results
            print(f"  Array modified by workers:")
            print(f"  Row sums: {[row.sum() for row in shared_np_array]:.2f}")
            
            # Cleanup
            shm.close()
            shm.unlink()
            
            print("✓ SharedMemory enables zero-copy sharing of large arrays")
    
    def demonstrate_manager(self):
        """Demonstrate Manager for shared objects."""
        print(f"\n{'='*60}")
        print("MANAGER OBJECTS")
        print(f"{'='*60}")
        
        # Create manager
        manager = mp.Manager()
        
        # Shared objects
        shared_dict = manager.dict()
        shared_list = manager.list()
        shared_namespace = manager.Namespace()
        
        # Initialize
        shared_namespace.counter = 0
        shared_namespace.status = "initialized"
        
        def worker_with_manager(shared_dict, shared_list, namespace, worker_id):
            """Worker that uses manager objects."""
            pid = os.getpid()
            
            # Modify shared dict
            shared_dict[f'worker_{worker_id}'] = {
                'pid': pid,
                'timestamp': datetime.now().isoformat(),
                'random': random.randint(1, 100)
            }
            
            # Append to shared list
            shared_list.append(f"Worker {worker_id} was here")
            
            # Update namespace
            namespace.counter += 1
            namespace.status = f"Updated by worker {worker_id}"
            
            print(f"  Worker {worker_id} (PID {pid}): Updated shared objects")
            
            # Simulate some work
            time.sleep(random.uniform(0.1, 0.3))
        
        # Start workers
        processes = []
        for i in range(4):
            p = mp.Process(target=worker_with_manager,
                          args=(shared_dict, shared_list, shared_namespace, i))
            p.start()
            processes.append(p)
        
        for p in processes:
            p.join()
        
        # Show results
        print("\nShared objects after all workers:")
        print(f"  Dict keys: {list(shared_dict.keys())}")
        print(f"  List length: {len(shared_list)}")
        print(f"  List contents: {list(shared_list)}")
        print(f"  Namespace counter: {shared_namespace.counter}")
        print(f"  Namespace status: {shared_namespace.status}")
        
        print("\n✓ Manager objects provide high-level shared data structures")
        print("  Note: Manager objects have higher overhead than Value/Array")
    
    def benchmark_ipc_methods(self):
        """Benchmark different IPC methods."""
        print(f"\n{'='*60}")
        print("IPC PERFORMANCE COMPARISON")
        print(f"{'='*60}")
        
        iterations = 1000
        data_sizes = [100, 1000, 10000]  # bytes
        
        def measure_pipe_throughput(size, iterations):
            """Measure pipe throughput."""
            parent_conn, child_conn = mp.Pipe()
            data = b'x' * size
            
            def sender(conn, data, iterations):
                for _ in range(iterations):
                    conn.send(data)
                conn.send(None)
            
            p = mp.Process(target=sender, args=(child_conn, data, iterations))
            
            start = time.perf_counter()
            p.start()
            
            count = 0
            while True:
                msg = parent_conn.recv()
                if msg is None:
                    break
                count += 1
            
            p.join()
            elapsed = time.perf_counter() - start
            
            return elapsed, count
        
        def measure_queue_throughput(size, iterations):
            """Measure queue throughput."""
            q = mp.Queue()
            data = b'x' * size
            
            def sender(queue, data, iterations):
                for _ in range(iterations):
                    queue.put(data)
                queue.put(None)
            
            p = mp.Process(target=sender, args=(q, data, iterations))
            
            start = time.perf_counter()
            p.start()
            
            count = 0
            while True:
                msg = q.get()
                if msg is None:
                    break
                count += 1
            
            p.join()
            elapsed = time.perf_counter() - start
            
            return elapsed, count
        
        print(f"\nThroughput test: {iterations} messages")
        print(f"{'Size (bytes)':<15} {'Pipe (msg/s)':<15} {'Queue (msg/s)':<15} {'Pipe/Queue':<10}")
        print("-" * 55)
        
        for size in data_sizes:
            # Measure pipe
            pipe_time, pipe_count = measure_pipe_throughput(size, iterations)
            pipe_throughput = pipe_count / pipe_time
            
            # Measure queue
            queue_time, queue_count = measure_queue_throughput(size, iterations)
            queue_throughput = queue_count / queue_time
            
            ratio = pipe_throughput / queue_throughput
            
            print(f"{size:<15} {pipe_throughput:<15.0f} {queue_throughput:<15.0f} {ratio:<10.2f}x")
        
        print("\n✓ Pipes are generally faster for simple point-to-point communication")
        print("✓ Queues provide more features (thread-safe, multiple consumers)")

def demonstrate_producer_consumer_pattern():
    """Demonstrate a complete producer-consumer pattern."""
    print(f"\n{'='*60}")
    print("PRODUCER-CONSUMER PATTERN")
    print(f"{'='*60}")
    
    # Configuration
    num_producers = 2
    num_consumers = 3
    items_per_producer = 5
    
    # Shared queue and stats
    task_queue = mp.Queue()
    result_queue = mp.Queue()
    
    def producer(producer_id, num_items, task_queue):
        """Producer that generates tasks."""
        print(f"  Producer {producer_id} started")
        
        for i in range(num_items):
            task = {
                'id': f"P{producer_id}-T{i}",
                'data': random.randint(1, 100),
                'producer': producer_id
            }
            task_queue.put(task)
            print(f"  Producer {producer_id}: Created task {task['id']}")
            time.sleep(random.uniform(0.1, 0.3))
        
        print(f"  Producer {producer_id} finished")
    
    def consumer(consumer_id, task_queue, result_queue):
        """Consumer that processes tasks."""
        print(f"  Consumer {consumer_id} started")
        processed = 0
        
        while True:
            try:
                task = task_queue.get(timeout=2)
                
                # Process task
                result = {
                    'task_id': task['id'],
                    'result': task['data'] ** 2,
                    'consumer': consumer_id,
                    'timestamp': time.time()
                }
                
                print(f"  Consumer {consumer_id}: Processed {task['id']} -> {result['result']}")
                result_queue.put(result)
                processed += 1
                
                time.sleep(random.uniform(0.2, 0.4))
                
            except queue.Empty:
                print(f"  Consumer {consumer_id}: No more tasks, exiting")
                break
        
        print(f"  Consumer {consumer_id} finished (processed {processed} tasks)")
    
    # Start producers
    producers = []
    for i in range(num_producers):
        p = mp.Process(target=producer, args=(i, items_per_producer, task_queue))
        p.start()
        producers.append(p)
    
    # Start consumers
    consumers = []
    for i in range(num_consumers):
        p = mp.Process(target=consumer, args=(i, task_queue, result_queue))
        p.start()
        consumers.append(p)
    
    # Wait for producers to finish
    for p in producers:
        p.join()
    
    print("\n  All producers finished")
    
    # Wait for consumers to finish
    for p in consumers:
        p.join()
    
    print("  All consumers finished")
    
    # Collect results
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    print(f"\n  Total tasks processed: {len(results)}")
    print(f"  Expected: {num_producers * items_per_producer}")
    
    # Show task distribution
    consumer_counts = {}
    for result in results:
        consumer_id = result['consumer']
        consumer_counts[consumer_id] = consumer_counts.get(consumer_id, 0) + 1
    
    print("\n  Task distribution:")
    for consumer_id, count in sorted(consumer_counts.items()):
        print(f"    Consumer {consumer_id}: {count} tasks")

def main():
    """Run IPC demonstrations."""
    print("Inter-Process Communication (IPC) Mechanisms")
    print("=" * 60)
    
    demo = IPCDemo()
    
    try:
        # 1. Pipes
        demo.demonstrate_pipes()
        
        # 2. Queues
        demo.demonstrate_queues()
        
        # 3. Shared memory
        demo.demonstrate_shared_memory()
        
        # 4. Manager objects
        demo.demonstrate_manager()
        
        # 5. Performance comparison
        demo.benchmark_ipc_methods()
        
        # 6. Producer-consumer pattern
        demonstrate_producer_consumer_pattern()
        
        # Summary
        print(f"\n{'='*60}")
        print("IPC MECHANISMS SUMMARY")
        print(f"{'='*60}")
        print("\n1. Pipes: Fast, simple, point-to-point communication")
        print("2. Queues: Thread-safe, multiple producers/consumers")
        print("3. Shared Memory: Zero-copy for large data")
        print("4. Manager: High-level shared objects with overhead")
        print("\nChoose based on your needs:")
        print("- Simple communication: Pipes")
        print("- Multiple producers/consumers: Queues") 
        print("- Large data sharing: Shared Memory")
        print("- Complex shared state: Manager objects")
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    mp.freeze_support()
    main()
