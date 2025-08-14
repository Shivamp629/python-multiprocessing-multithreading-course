#!/usr/bin/env python3
"""
Async I/O Basics: Understanding coroutines and event loops

This script demonstrates fundamental async/await concepts and how
the event loop schedules and executes coroutines.
"""

import asyncio
import time
import sys
from datetime import datetime
import threading

class AsyncBasicsDemo:
    """Demonstrates basic async/await concepts."""
    
    def __init__(self):
        self.event_count = 0
    
    async def simple_coroutine(self, name, duration):
        """A simple coroutine that simulates work."""
        print(f"[{name}] Coroutine started at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
        
        # This is where the coroutine yields control back to the event loop
        await asyncio.sleep(duration)
        
        print(f"[{name}] Coroutine finished after {duration}s")
        return f"{name} result"
    
    async def demonstrate_basics(self):
        """Demonstrate basic async concepts."""
        print(f"\n{'='*60}")
        print("ASYNC/AWAIT BASICS")
        print(f"{'='*60}")
        
        # 1. Creating and awaiting a coroutine
        print("\n1. Simple coroutine execution:")
        result = await self.simple_coroutine("Task-1", 1)
        print(f"   Result: {result}")
        
        # 2. Running multiple coroutines sequentially
        print("\n2. Sequential execution (NOT concurrent):")
        start = time.perf_counter()
        
        result1 = await self.simple_coroutine("Task-A", 1)
        result2 = await self.simple_coroutine("Task-B", 1)
        result3 = await self.simple_coroutine("Task-C", 1)
        
        elapsed = time.perf_counter() - start
        print(f"   Total time: {elapsed:.2f}s (sequential = sum of all)")
        
        # 3. Running coroutines concurrently
        print("\n3. Concurrent execution with gather:")
        start = time.perf_counter()
        
        results = await asyncio.gather(
            self.simple_coroutine("Task-X", 1),
            self.simple_coroutine("Task-Y", 1),
            self.simple_coroutine("Task-Z", 1)
        )
        
        elapsed = time.perf_counter() - start
        print(f"   Results: {results}")
        print(f"   Total time: {elapsed:.2f}s (concurrent = max duration)")
        
        # 4. Creating tasks for more control
        print("\n4. Using create_task for concurrent execution:")
        
        # Create tasks (schedules them immediately)
        task1 = asyncio.create_task(self.simple_coroutine("Task-1", 2))
        task2 = asyncio.create_task(self.simple_coroutine("Task-2", 1))
        task3 = asyncio.create_task(self.simple_coroutine("Task-3", 1.5))
        
        print("   Tasks created and scheduled")
        
        # Do other work while tasks run
        await asyncio.sleep(0.5)
        print("   Main coroutine doing other work...")
        
        # Wait for tasks to complete
        results = await asyncio.gather(task1, task2, task3)
        print(f"   All tasks complete: {results}")
    
    async def demonstrate_event_loop(self):
        """Show how the event loop works."""
        print(f"\n{'='*60}")
        print("EVENT LOOP DEMONSTRATION")
        print(f"{'='*60}")
        
        # Get current event loop
        loop = asyncio.get_running_loop()
        print(f"\nEvent loop info:")
        print(f"  Running: {loop.is_running()}")
        print(f"  Thread: {threading.current_thread().name}")
        
        # Schedule callbacks
        print("\n1. Scheduling callbacks:")
        
        def callback(n):
            print(f"   Callback {n} executed at {time.time():.2f}")
        
        # Schedule immediate callback
        loop.call_soon(callback, 1)
        
        # Schedule delayed callbacks
        loop.call_later(0.5, callback, 2)
        loop.call_later(1.0, callback, 3)
        
        print("   Callbacks scheduled, waiting...")
        await asyncio.sleep(1.5)
        
        # 2. Show event loop iterations
        print("\n2. Event loop iterations:")
        
        async def track_iterations():
            for i in range(5):
                print(f"   Iteration {i}, time: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                # Yield control to event loop
                await asyncio.sleep(0)
        
        await track_iterations()
    
    async def demonstrate_coroutine_states(self):
        """Show different states of coroutines."""
        print(f"\n{'='*60}")
        print("COROUTINE STATES")
        print(f"{'='*60}")
        
        async def stateful_coroutine(name):
            print(f"  [{name}] State 1: Started")
            
            await asyncio.sleep(0.5)
            print(f"  [{name}] State 2: After first await")
            
            await asyncio.sleep(0.5)
            print(f"  [{name}] State 3: After second await")
            
            return f"{name} complete"
        
        # Create coroutine object (not running yet)
        coro = stateful_coroutine("Coro-1")
        print(f"\n1. Coroutine object created: {coro}")
        print(f"   Type: {type(coro)}")
        
        # Create task (schedules coroutine)
        task = asyncio.create_task(coro)
        print(f"\n2. Task created: {task}")
        print(f"   State: {task._state}")
        
        # Let it run a bit
        await asyncio.sleep(0.1)
        print(f"\n3. Task after 0.1s:")
        print(f"   Done: {task.done()}")
        
        # Wait for completion
        result = await task
        print(f"\n4. Task completed:")
        print(f"   Done: {task.done()}")
        print(f"   Result: {result}")
    
    async def demonstrate_concurrency_vs_parallelism(self):
        """Show the difference between concurrency and parallelism."""
        print(f"\n{'='*60}")
        print("CONCURRENCY VS PARALLELISM")
        print(f"{'='*60}")
        
        async def cpu_bound_async(n, task_id):
            """CPU-bound task (still runs in single thread)."""
            print(f"  Task {task_id}: Computing sum of {n} numbers")
            start = time.perf_counter()
            
            # This blocks the event loop!
            result = sum(i * i for i in range(n))
            
            elapsed = time.perf_counter() - start
            print(f"  Task {task_id}: Computed {result} in {elapsed:.3f}s")
            return result
        
        async def io_bound_async(duration, task_id):
            """I/O-bound task (yields to event loop)."""
            print(f"  Task {task_id}: Waiting {duration}s (I/O simulation)")
            start = time.perf_counter()
            
            # This yields control to event loop
            await asyncio.sleep(duration)
            
            elapsed = time.perf_counter() - start
            print(f"  Task {task_id}: Waited {elapsed:.3f}s")
            return f"IO-{task_id}"
        
        # Test I/O bound tasks (true concurrency)
        print("\n1. I/O-bound tasks (concurrent):")
        start = time.perf_counter()
        
        io_results = await asyncio.gather(
            io_bound_async(1, "A"),
            io_bound_async(1, "B"),
            io_bound_async(1, "C")
        )
        
        io_elapsed = time.perf_counter() - start
        print(f"   Total time: {io_elapsed:.2f}s (runs concurrently)")
        
        # Test CPU bound tasks (no parallelism)
        print("\n2. CPU-bound tasks (NO parallelism):")
        start = time.perf_counter()
        
        cpu_results = await asyncio.gather(
            cpu_bound_async(1000000, "X"),
            cpu_bound_async(1000000, "Y"),
            cpu_bound_async(1000000, "Z")
        )
        
        cpu_elapsed = time.perf_counter() - start
        print(f"   Total time: {cpu_elapsed:.2f}s (runs sequentially)")
        
        print(f"\n✓ Async provides concurrency, not parallelism")
        print(f"✓ I/O tasks benefit from async, CPU tasks don't")
    
    async def demonstrate_async_iteration(self):
        """Show async generators and iteration."""
        print(f"\n{'='*60}")
        print("ASYNC ITERATION")
        print(f"{'='*60}")
        
        # Async generator
        async def async_counter(start, end, delay):
            """Async generator that yields numbers."""
            for i in range(start, end):
                await asyncio.sleep(delay)
                yield i
        
        # Async iteration
        print("\n1. Async for loop:")
        async for num in async_counter(1, 5, 0.5):
            print(f"   Received: {num} at {datetime.now().strftime('%H:%M:%S')}")
        
        # Async comprehension
        print("\n2. Async comprehension:")
        start = time.perf_counter()
        
        # This still runs sequentially within the comprehension
        results = [num async for num in async_counter(10, 15, 0.2)]
        
        elapsed = time.perf_counter() - start
        print(f"   Results: {results}")
        print(f"   Time: {elapsed:.2f}s")
        
        # Async context manager
        print("\n3. Async context manager:")
        
        class AsyncResource:
            async def __aenter__(self):
                print("   Acquiring async resource...")
                await asyncio.sleep(0.5)
                print("   Resource acquired")
                return self
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                print("   Releasing async resource...")
                await asyncio.sleep(0.5)
                print("   Resource released")
            
            async def use(self):
                print("   Using resource")
                await asyncio.sleep(0.2)
        
        async with AsyncResource() as resource:
            await resource.use()

def demonstrate_sync_vs_async():
    """Compare synchronous vs asynchronous execution."""
    print(f"\n{'='*60}")
    print("SYNC VS ASYNC COMPARISON")
    print(f"{'='*60}")
    
    # Synchronous version
    def sync_fetch(url, duration):
        print(f"  Sync: Fetching {url}")
        time.sleep(duration)
        return f"Data from {url}"
    
    # Asynchronous version
    async def async_fetch(url, duration):
        print(f"  Async: Fetching {url}")
        await asyncio.sleep(duration)
        return f"Data from {url}"
    
    # Test synchronous
    print("\n1. Synchronous execution:")
    start = time.perf_counter()
    
    results = []
    for i in range(3):
        result = sync_fetch(f"url{i}", 1)
        results.append(result)
    
    sync_time = time.perf_counter() - start
    print(f"   Results: {results}")
    print(f"   Time: {sync_time:.2f}s")
    
    # Test asynchronous
    print("\n2. Asynchronous execution:")
    
    async def async_main():
        start = time.perf_counter()
        
        results = await asyncio.gather(
            async_fetch("url0", 1),
            async_fetch("url1", 1),
            async_fetch("url2", 1)
        )
        
        async_time = time.perf_counter() - start
        print(f"   Results: {results}")
        print(f"   Time: {async_time:.2f}s")
        print(f"   Speedup: {sync_time/async_time:.1f}x")
    
    asyncio.run(async_main())

async def main():
    """Run all async demonstrations."""
    demo = AsyncBasicsDemo()
    
    try:
        await demo.demonstrate_basics()
        await demo.demonstrate_event_loop()
        await demo.demonstrate_coroutine_states()
        await demo.demonstrate_concurrency_vs_parallelism()
        await demo.demonstrate_async_iteration()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("Async I/O Basics Demonstration")
    print(f"Python version: {sys.version}")
    print("=" * 60)
    
    # Run async main
    asyncio.run(main())
    
    # Run sync vs async comparison
    demonstrate_sync_vs_async()
    
    print(f"\n{'='*60}")
    print("KEY TAKEAWAYS")
    print(f"{'='*60}")
    print("1. Coroutines are functions that can be paused and resumed")
    print("2. The event loop schedules and executes coroutines")
    print("3. 'await' yields control back to the event loop")
    print("4. Async excels at I/O concurrency, not CPU parallelism")
    print("5. Use gather() or create_task() for concurrent execution")
