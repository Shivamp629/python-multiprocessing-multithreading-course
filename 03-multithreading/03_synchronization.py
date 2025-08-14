#!/usr/bin/env python3
"""
Thread Synchronization Primitives

This script demonstrates various synchronization mechanisms for coordinating
threads and preventing race conditions.
"""

import threading
import time
import random
import queue
from collections import deque, defaultdict
from contextlib import contextmanager
from datetime import datetime
import sys

class ColoredOutput:
    """Helper for colored terminal output."""
    COLORS = {
        'red': '\033[91m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'blue': '\033[94m',
        'magenta': '\033[95m',
        'cyan': '\033[96m',
        'white': '\033[97m',
        'reset': '\033[0m'
    }
    
    @classmethod
    def print(cls, message, color='white'):
        """Print colored message."""
        print(f"{cls.COLORS[color]}{message}{cls.COLORS['reset']}")

class RaceConditionDemo:
    """Demonstrates race conditions and their solutions."""
    
    def __init__(self):
        self.counter = 0
        self.lock = threading.Lock()
        
    def unsafe_increment(self, iterations):
        """Increment counter without synchronization (unsafe)."""
        for _ in range(iterations):
            # Read-modify-write without protection
            temp = self.counter
            temp += 1
            # Simulate some processing time to increase chance of race
            time.sleep(0.000001)
            self.counter = temp
    
    def safe_increment(self, iterations):
        """Increment counter with lock (safe)."""
        for _ in range(iterations):
            with self.lock:
                temp = self.counter
                temp += 1
                time.sleep(0.000001)
                self.counter = temp
    
    def demonstrate(self):
        """Show race condition and solution."""
        ColoredOutput.print("\n" + "="*60, 'cyan')
        ColoredOutput.print("RACE CONDITION DEMONSTRATION", 'cyan')
        ColoredOutput.print("="*60, 'cyan')
        
        iterations = 1000
        num_threads = 5
        
        # Test 1: Without synchronization
        print("\n1. Without synchronization (unsafe):")
        self.counter = 0
        threads = []
        
        start = time.perf_counter()
        for i in range(num_threads):
            t = threading.Thread(target=self.unsafe_increment, args=(iterations,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        elapsed = time.perf_counter() - start
        expected = num_threads * iterations
        
        ColoredOutput.print(f"   Expected: {expected}", 'green')
        ColoredOutput.print(f"   Actual: {self.counter}", 'red')
        ColoredOutput.print(f"   Lost updates: {expected - self.counter}", 'yellow')
        print(f"   Time: {elapsed:.3f} seconds")
        
        # Test 2: With synchronization
        print("\n2. With lock synchronization (safe):")
        self.counter = 0
        threads = []
        
        start = time.perf_counter()
        for i in range(num_threads):
            t = threading.Thread(target=self.safe_increment, args=(iterations,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        elapsed = time.perf_counter() - start
        
        ColoredOutput.print(f"   Expected: {expected}", 'green')
        ColoredOutput.print(f"   Actual: {self.counter}", 'green')
        print(f"   Time: {elapsed:.3f} seconds (slower due to lock overhead)")

class LockDemo:
    """Demonstrates different types of locks."""
    
    def demonstrate_lock(self):
        """Basic Lock demonstration."""
        ColoredOutput.print("\n" + "="*60, 'cyan')
        ColoredOutput.print("LOCK (MUTEX) DEMONSTRATION", 'cyan')
        ColoredOutput.print("="*60, 'cyan')
        
        shared_resource = []
        lock = threading.Lock()
        
        def worker(worker_id, operations):
            for i in range(operations):
                # Acquire lock
                lock.acquire()
                try:
                    # Critical section
                    shared_resource.append(f"Worker-{worker_id}-Op-{i}")
                    print(f"   Worker {worker_id}: Performed operation {i}")
                    time.sleep(0.01)  # Simulate work
                finally:
                    # Always release lock
                    lock.release()
        
        # Create workers
        threads = []
        for i in range(3):
            t = threading.Thread(target=worker, args=(i, 3))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        print(f"\nShared resource contains {len(shared_resource)} items")
        print("All operations completed safely with mutual exclusion")
    
    def demonstrate_rlock(self):
        """RLock (Reentrant Lock) demonstration."""
        ColoredOutput.print("\n" + "="*60, 'cyan')
        ColoredOutput.print("RLOCK (REENTRANT LOCK) DEMONSTRATION", 'cyan')
        ColoredOutput.print("="*60, 'cyan')
        
        rlock = threading.RLock()
        
        def recursive_function(n, thread_id):
            with rlock:
                print(f"   Thread {thread_id}: Acquired lock, depth={n}")
                if n > 0:
                    # Same thread can acquire again
                    recursive_function(n - 1, thread_id)
                print(f"   Thread {thread_id}: Releasing lock, depth={n}")
        
        # This would deadlock with regular Lock but works with RLock
        threads = []
        for i in range(2):
            t = threading.Thread(target=recursive_function, args=(3, i))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        print("\nRLock allows same thread to acquire multiple times")

class SemaphoreDemo:
    """Demonstrates Semaphore for resource limiting."""
    
    def demonstrate(self):
        ColoredOutput.print("\n" + "="*60, 'cyan')
        ColoredOutput.print("SEMAPHORE DEMONSTRATION", 'cyan')
        ColoredOutput.print("="*60, 'cyan')
        
        # Limit concurrent access to 3
        max_connections = 3
        connection_pool = threading.Semaphore(max_connections)
        active_connections = []
        active_lock = threading.Lock()
        
        def simulate_database_query(query_id):
            thread_name = threading.current_thread().name
            
            # Acquire semaphore (get connection from pool)
            print(f"   Query {query_id}: Waiting for connection...")
            connection_pool.acquire()
            
            with active_lock:
                active_connections.append(query_id)
                ColoredOutput.print(
                    f"   Query {query_id}: Got connection! "
                    f"Active: {active_connections}", 'green'
                )
            
            # Simulate query execution
            time.sleep(random.uniform(0.5, 1.5))
            
            # Release semaphore (return connection to pool)
            with active_lock:
                active_connections.remove(query_id)
                ColoredOutput.print(
                    f"   Query {query_id}: Released connection. "
                    f"Active: {active_connections}", 'yellow'
                )
            
            connection_pool.release()
        
        # Create many queries
        threads = []
        for i in range(10):
            t = threading.Thread(target=simulate_database_query, args=(i,))
            threads.append(t)
            t.start()
            time.sleep(0.1)  # Stagger starts
        
        for t in threads:
            t.join()
        
        print(f"\nSemaphore limited concurrent access to {max_connections}")

class EventDemo:
    """Demonstrates Event for thread signaling."""
    
    def demonstrate(self):
        ColoredOutput.print("\n" + "="*60, 'cyan')
        ColoredOutput.print("EVENT DEMONSTRATION", 'cyan')
        ColoredOutput.print("="*60, 'cyan')
        
        # Create events
        data_ready = threading.Event()
        shutdown = threading.Event()
        
        shared_data = {'value': None}
        
        def producer():
            """Produces data and signals when ready."""
            print("   Producer: Starting data generation...")
            time.sleep(2)  # Simulate data preparation
            
            shared_data['value'] = random.randint(1, 100)
            ColoredOutput.print(
                f"   Producer: Data ready! Value = {shared_data['value']}", 
                'green'
            )
            
            # Signal that data is ready
            data_ready.set()
            
            # Wait for shutdown signal
            shutdown.wait()
            print("   Producer: Shutting down")
        
        def consumer(consumer_id):
            """Waits for data and processes it."""
            print(f"   Consumer {consumer_id}: Waiting for data...")
            
            # Wait for data to be ready
            data_ready.wait()
            
            # Process data
            value = shared_data['value']
            result = value * consumer_id
            ColoredOutput.print(
                f"   Consumer {consumer_id}: Processed data: {value} * {consumer_id} = {result}",
                'blue'
            )
            
            time.sleep(0.5)  # Simulate processing
        
        # Start producer
        producer_thread = threading.Thread(target=producer)
        producer_thread.start()
        
        # Start consumers
        consumer_threads = []
        for i in range(3):
            t = threading.Thread(target=consumer, args=(i,))
            consumer_threads.append(t)
            t.start()
        
        # Wait for consumers
        for t in consumer_threads:
            t.join()
        
        # Signal shutdown
        shutdown.set()
        producer_thread.join()
        
        print("\nEvent coordinated producer-consumer communication")

class ConditionDemo:
    """Demonstrates Condition for complex synchronization."""
    
    def demonstrate(self):
        ColoredOutput.print("\n" + "="*60, 'cyan')
        ColoredOutput.print("CONDITION VARIABLE DEMONSTRATION", 'cyan')
        ColoredOutput.print("="*60, 'cyan')
        
        # Bounded buffer implementation
        buffer = deque(maxlen=5)
        condition = threading.Condition()
        
        def producer(producer_id):
            """Produces items and adds to buffer."""
            for i in range(5):
                item = f"P{producer_id}-Item{i}"
                
                with condition:
                    # Wait while buffer is full
                    while len(buffer) >= buffer.maxlen:
                        print(f"   Producer {producer_id}: Buffer full, waiting...")
                        condition.wait()
                    
                    # Add item
                    buffer.append(item)
                    ColoredOutput.print(
                        f"   Producer {producer_id}: Added {item}. "
                        f"Buffer: {list(buffer)}", 'green'
                    )
                    
                    # Notify consumers
                    condition.notify_all()
                
                time.sleep(random.uniform(0.1, 0.3))
        
        def consumer(consumer_id):
            """Consumes items from buffer."""
            consumed = []
            
            while len(consumed) < 5:
                with condition:
                    # Wait while buffer is empty
                    while not buffer:
                        print(f"   Consumer {consumer_id}: Buffer empty, waiting...")
                        condition.wait()
                    
                    # Remove item
                    item = buffer.popleft()
                    consumed.append(item)
                    ColoredOutput.print(
                        f"   Consumer {consumer_id}: Consumed {item}. "
                        f"Buffer: {list(buffer)}", 'blue'
                    )
                    
                    # Notify producers
                    condition.notify_all()
                
                time.sleep(random.uniform(0.2, 0.4))
            
            print(f"   Consumer {consumer_id}: Finished. Consumed: {consumed}")
        
        # Start threads
        threads = []
        
        # Producers
        for i in range(2):
            t = threading.Thread(target=producer, args=(i,))
            threads.append(t)
            t.start()
        
        # Consumers
        for i in range(2):
            t = threading.Thread(target=consumer, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        print("\nCondition variable coordinated bounded buffer")

class BarrierDemo:
    """Demonstrates Barrier for synchronizing thread phases."""
    
    def demonstrate(self):
        ColoredOutput.print("\n" + "="*60, 'cyan')
        ColoredOutput.print("BARRIER DEMONSTRATION", 'cyan')
        ColoredOutput.print("="*60, 'cyan')
        
        num_workers = 4
        barrier = threading.Barrier(num_workers)
        results = defaultdict(list)
        results_lock = threading.Lock()
        
        def worker(worker_id):
            """Worker that goes through multiple phases."""
            # Phase 1: Initialization
            print(f"   Worker {worker_id}: Starting initialization...")
            time.sleep(random.uniform(0.5, 1.5))
            print(f"   Worker {worker_id}: Initialization complete")
            
            # Wait for all workers to finish phase 1
            try:
                index = barrier.wait()
                if index == 0:  # One thread elected as coordinator
                    ColoredOutput.print(
                        "\n   === All workers finished Phase 1 ===\n", 'yellow'
                    )
            except threading.BrokenBarrierError:
                print(f"   Worker {worker_id}: Barrier broken!")
                return
            
            # Phase 2: Processing
            print(f"   Worker {worker_id}: Starting processing...")
            result = worker_id * random.randint(10, 20)
            time.sleep(random.uniform(0.5, 1.0))
            
            with results_lock:
                results['phase2'].append(result)
            
            print(f"   Worker {worker_id}: Processing complete, result={result}")
            
            # Wait for all workers to finish phase 2
            try:
                index = barrier.wait()
                if index == 0:
                    ColoredOutput.print(
                        "\n   === All workers finished Phase 2 ===", 'yellow'
                    )
                    with results_lock:
                        total = sum(results['phase2'])
                        ColoredOutput.print(
                            f"   Combined result: {results['phase2']} = {total}\n",
                            'green'
                        )
            except threading.BrokenBarrierError:
                print(f"   Worker {worker_id}: Barrier broken!")
                return
            
            # Phase 3: Cleanup
            print(f"   Worker {worker_id}: Performing cleanup...")
            time.sleep(0.2)
            print(f"   Worker {worker_id}: Done!")
        
        # Start workers
        threads = []
        for i in range(num_workers):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        print("\nBarrier synchronized workers through multiple phases")

class DeadlockDemo:
    """Demonstrates deadlock scenarios and prevention."""
    
    def demonstrate(self):
        ColoredOutput.print("\n" + "="*60, 'cyan')
        ColoredOutput.print("DEADLOCK PREVENTION DEMONSTRATION", 'cyan')
        ColoredOutput.print("="*60, 'cyan')
        
        # Resources
        resource_a = threading.Lock()
        resource_b = threading.Lock()
        
        def worker_deadlock_prone(worker_id, first_lock, second_lock):
            """Worker that can cause deadlock."""
            name = threading.current_thread().name
            
            print(f"   {name}: Acquiring first lock...")
            with first_lock:
                print(f"   {name}: Got first lock")
                time.sleep(0.1)  # Increase chance of deadlock
                
                print(f"   {name}: Acquiring second lock...")
                acquired = second_lock.acquire(timeout=2)
                
                if acquired:
                    try:
                        print(f"   {name}: Got both locks!")
                        time.sleep(0.1)
                    finally:
                        second_lock.release()
                else:
                    ColoredOutput.print(
                        f"   {name}: DEADLOCK DETECTED! Couldn't acquire second lock",
                        'red'
                    )
        
        # Test 1: Deadlock scenario
        print("\n1. Deadlock-prone scenario (different lock order):")
        t1 = threading.Thread(
            target=worker_deadlock_prone,
            args=(1, resource_a, resource_b),
            name="Worker-1"
        )
        t2 = threading.Thread(
            target=worker_deadlock_prone,
            args=(2, resource_b, resource_a),
            name="Worker-2"
        )
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        # Test 2: Deadlock prevention
        print("\n2. Deadlock prevention (ordered lock acquisition):")
        
        def worker_safe(worker_id):
            """Worker that prevents deadlock by ordering locks."""
            name = threading.current_thread().name
            
            # Always acquire locks in same order (by id)
            locks = sorted([resource_a, resource_b], key=id)
            
            print(f"   {name}: Acquiring locks in order...")
            with locks[0]:
                with locks[1]:
                    ColoredOutput.print(
                        f"   {name}: Got both locks safely!",
                        'green'
                    )
                    time.sleep(0.1)
        
        t1 = threading.Thread(target=worker_safe, args=(1,), name="Worker-3")
        t2 = threading.Thread(target=worker_safe, args=(2,), name="Worker-4")
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        print("\nOrdered lock acquisition prevents deadlock")

def main():
    """Run synchronization demonstrations."""
    print("Thread Synchronization Primitives Demonstration")
    print(f"Python version: {sys.version}")
    
    try:
        # Demonstrate race conditions
        race_demo = RaceConditionDemo()
        race_demo.demonstrate()
        
        # Demonstrate locks
        lock_demo = LockDemo()
        lock_demo.demonstrate_lock()
        lock_demo.demonstrate_rlock()
        
        # Demonstrate semaphore
        sem_demo = SemaphoreDemo()
        sem_demo.demonstrate()
        
        # Demonstrate event
        event_demo = EventDemo()
        event_demo.demonstrate()
        
        # Demonstrate condition
        cond_demo = ConditionDemo()
        cond_demo.demonstrate()
        
        # Demonstrate barrier
        barrier_demo = BarrierDemo()
        barrier_demo.demonstrate()
        
        # Demonstrate deadlock
        deadlock_demo = DeadlockDemo()
        deadlock_demo.demonstrate()
        
        # Summary
        ColoredOutput.print("\n" + "="*60, 'cyan')
        ColoredOutput.print("SYNCHRONIZATION PRIMITIVES SUMMARY", 'cyan')
        ColoredOutput.print("="*60, 'cyan')
        
        print("\n1. Lock/RLock: Mutual exclusion for critical sections")
        print("2. Semaphore: Limit concurrent access to resources")
        print("3. Event: Simple signaling between threads")
        print("4. Condition: Complex wait/notify patterns")
        print("5. Barrier: Synchronize threads at checkpoints")
        print("6. Queue: Thread-safe data exchange")
        print("\nAlways consider deadlock prevention strategies!")
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
