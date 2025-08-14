#!/usr/bin/env python3
"""
Async Patterns and Best Practices

This script demonstrates advanced async patterns, error handling,
and best practices for building robust async applications.
"""

import asyncio
import aiohttp
import time
import random
from typing import List, Dict, Any, Optional, Callable, TypeVar, Coroutine
from functools import wraps
from contextlib import asynccontextmanager
import logging
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

T = TypeVar('T')

class AsyncPatterns:
    """Demonstrates various async patterns and best practices."""
    
    # Pattern 1: Retry with exponential backoff
    @staticmethod
    async def retry_with_backoff(
        coro_func: Callable[..., Coroutine[Any, Any, T]],
        *args,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        backoff_factor: float = 2.0,
        max_delay: float = 60.0,
        **kwargs
    ) -> Optional[T]:
        """Retry an async operation with exponential backoff."""
        delay = initial_delay
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                return await coro_func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay:.1f} seconds..."
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)
                else:
                    logger.error(f"All {max_retries} attempts failed")
        
        raise last_exception
    
    # Pattern 2: Circuit breaker
    class CircuitBreaker:
        """Circuit breaker pattern for fault tolerance."""
        
        class State(Enum):
            CLOSED = "closed"
            OPEN = "open"
            HALF_OPEN = "half_open"
        
        def __init__(
            self,
            failure_threshold: int = 5,
            recovery_timeout: float = 60.0,
            expected_exception: type = Exception
        ):
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self.expected_exception = expected_exception
            self.failure_count = 0
            self.last_failure_time = None
            self.state = self.State.CLOSED
        
        async def call(self, coro_func: Callable, *args, **kwargs):
            """Execute function through circuit breaker."""
            if self.state == self.State.OPEN:
                if (time.time() - self.last_failure_time) > self.recovery_timeout:
                    self.state = self.State.HALF_OPEN
                else:
                    raise Exception("Circuit breaker is OPEN")
            
            try:
                result = await coro_func(*args, **kwargs)
                if self.state == self.State.HALF_OPEN:
                    self.state = self.State.CLOSED
                    self.failure_count = 0
                return result
            
            except self.expected_exception as e:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = self.State.OPEN
                    logger.error(f"Circuit breaker opened after {self.failure_count} failures")
                
                raise e
    
    # Pattern 3: Rate limiter
    class RateLimiter:
        """Token bucket rate limiter."""
        
        def __init__(self, rate: float, capacity: int):
            self.rate = rate  # tokens per second
            self.capacity = capacity
            self.tokens = capacity
            self.last_update = time.time()
            self.lock = asyncio.Lock()
        
        async def acquire(self, tokens: int = 1):
            """Acquire tokens, waiting if necessary."""
            async with self.lock:
                while True:
                    now = time.time()
                    # Add tokens based on time passed
                    elapsed = now - self.last_update
                    self.tokens = min(
                        self.capacity,
                        self.tokens + elapsed * self.rate
                    )
                    self.last_update = now
                    
                    if self.tokens >= tokens:
                        self.tokens -= tokens
                        return
                    
                    # Calculate wait time
                    wait_time = (tokens - self.tokens) / self.rate
                    await asyncio.sleep(wait_time)
    
    # Pattern 4: Async context manager for resource pooling
    @dataclass
    class PooledResource:
        """A pooled resource."""
        id: int
        in_use: bool = False
        created_at: datetime = None
        
        def __post_init__(self):
            self.created_at = datetime.now()
    
    class AsyncResourcePool:
        """Async resource pool with connection management."""
        
        def __init__(self, create_resource: Callable, max_size: int = 10):
            self.create_resource = create_resource
            self.max_size = max_size
            self.pool: List[PooledResource] = []
            self.available = asyncio.Queue(maxsize=max_size)
            self.lock = asyncio.Lock()
            self._closed = False
        
        async def acquire(self):
            """Acquire a resource from the pool."""
            if self._closed:
                raise RuntimeError("Pool is closed")
            
            # Try to get from available queue
            try:
                resource = self.available.get_nowait()
            except asyncio.QueueEmpty:
                # Create new resource if under limit
                async with self.lock:
                    if len(self.pool) < self.max_size:
                        resource = await self.create_resource()
                        pooled = PooledResource(id=len(self.pool))
                        pooled.resource = resource
                        self.pool.append(pooled)
                    else:
                        # Wait for available resource
                        pooled = await self.available.get()
                        resource = pooled.resource
            else:
                resource = resource.resource
            
            return resource
        
        async def release(self, resource):
            """Release a resource back to the pool."""
            # Find the pooled resource
            for pooled in self.pool:
                if hasattr(pooled, 'resource') and pooled.resource == resource:
                    await self.available.put(pooled)
                    break
        
        async def close(self):
            """Close all resources in the pool."""
            self._closed = True
            # Close all resources
            for pooled in self.pool:
                if hasattr(pooled, 'resource'):
                    await pooled.resource.close()
    
    # Pattern 5: Async queue processor
    class AsyncQueueProcessor:
        """Process items from a queue with multiple workers."""
        
        def __init__(
            self,
            process_func: Callable,
            num_workers: int = 5,
            queue_size: int = 100
        ):
            self.process_func = process_func
            self.num_workers = num_workers
            self.queue = asyncio.Queue(maxsize=queue_size)
            self.workers = []
            self.running = False
            self.stats = defaultdict(int)
        
        async def start(self):
            """Start worker tasks."""
            self.running = True
            for i in range(self.num_workers):
                worker = asyncio.create_task(self._worker(i))
                self.workers.append(worker)
            logger.info(f"Started {self.num_workers} workers")
        
        async def stop(self):
            """Stop all workers gracefully."""
            self.running = False
            
            # Send stop signals
            for _ in self.workers:
                await self.queue.put(None)
            
            # Wait for workers to finish
            await asyncio.gather(*self.workers)
            logger.info("All workers stopped")
        
        async def submit(self, item):
            """Submit an item for processing."""
            await self.queue.put(item)
            self.stats['submitted'] += 1
        
        async def _worker(self, worker_id: int):
            """Worker coroutine."""
            logger.info(f"Worker {worker_id} started")
            
            while self.running:
                try:
                    item = await self.queue.get()
                    
                    if item is None:  # Stop signal
                        break
                    
                    start = time.perf_counter()
                    try:
                        await self.process_func(item)
                        self.stats['processed'] += 1
                    except Exception as e:
                        logger.error(f"Worker {worker_id} error: {e}")
                        self.stats['failed'] += 1
                    
                    elapsed = time.perf_counter() - start
                    self.stats['total_time'] += elapsed
                    
                except Exception as e:
                    logger.error(f"Worker {worker_id} fatal error: {e}")
            
            logger.info(f"Worker {worker_id} stopped")

async def demonstrate_patterns():
    """Demonstrate various async patterns."""
    print(f"\n{'='*60}")
    print("ASYNC PATTERNS DEMONSTRATION")
    print(f"{'='*60}")
    
    patterns = AsyncPatterns()
    
    # 1. Retry with backoff
    print("\n1. Retry with exponential backoff:")
    
    async def flaky_operation():
        """Operation that fails 70% of the time."""
        if random.random() < 0.7:
            raise Exception("Random failure")
        return "Success!"
    
    try:
        result = await patterns.retry_with_backoff(
            flaky_operation,
            max_retries=5,
            initial_delay=0.5
        )
        print(f"   Result: {result}")
    except Exception as e:
        print(f"   Failed after retries: {e}")
    
    # 2. Circuit breaker
    print("\n2. Circuit breaker pattern:")
    
    breaker = patterns.CircuitBreaker(failure_threshold=3, recovery_timeout=2.0)
    
    async def unreliable_service():
        """Service that always fails."""
        raise Exception("Service unavailable")
    
    for i in range(5):
        try:
            await breaker.call(unreliable_service)
        except Exception as e:
            print(f"   Call {i+1}: {e} (State: {breaker.state.value})")
        await asyncio.sleep(0.5)
    
    # 3. Rate limiter
    print("\n3. Rate limiting pattern:")
    
    rate_limiter = patterns.RateLimiter(rate=5.0, capacity=10)  # 5 req/sec
    
    async def make_request(request_id):
        await rate_limiter.acquire()
        print(f"   Request {request_id} executed at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
    
    # Try to make 10 requests quickly
    start = time.perf_counter()
    await asyncio.gather(*[make_request(i) for i in range(10)])
    elapsed = time.perf_counter() - start
    print(f"   10 requests completed in {elapsed:.2f}s (rate limited)")
    
    # 4. Queue processor
    print("\n4. Async queue processor:")
    
    async def process_item(item):
        """Simulate item processing."""
        await asyncio.sleep(random.uniform(0.1, 0.3))
        logger.info(f"Processed item: {item}")
    
    processor = patterns.AsyncQueueProcessor(process_item, num_workers=3)
    await processor.start()
    
    # Submit items
    for i in range(10):
        await processor.submit(f"item_{i}")
    
    # Wait a bit for processing
    await asyncio.sleep(2)
    
    await processor.stop()
    print(f"   Stats: {dict(processor.stats)}")

async def demonstrate_error_handling():
    """Demonstrate proper error handling in async code."""
    print(f"\n{'='*60}")
    print("ASYNC ERROR HANDLING")
    print(f"{'='*60}")
    
    # 1. Exception propagation
    print("\n1. Exception propagation in gather:")
    
    async def failing_task(task_id, fail=False):
        await asyncio.sleep(0.1)
        if fail:
            raise ValueError(f"Task {task_id} failed")
        return f"Task {task_id} success"
    
    # Without return_exceptions
    try:
        results = await asyncio.gather(
            failing_task(1),
            failing_task(2, fail=True),
            failing_task(3)
        )
    except ValueError as e:
        print(f"   Caught exception: {e}")
    
    # With return_exceptions
    results = await asyncio.gather(
        failing_task(1),
        failing_task(2, fail=True),
        failing_task(3),
        return_exceptions=True
    )
    
    print("   Results with return_exceptions:")
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"     Task {i+1}: Failed - {result}")
        else:
            print(f"     Task {i+1}: {result}")
    
    # 2. Timeout handling
    print("\n2. Timeout handling:")
    
    async def slow_operation():
        await asyncio.sleep(2)
        return "Completed"
    
    try:
        result = await asyncio.wait_for(slow_operation(), timeout=1.0)
    except asyncio.TimeoutError:
        print("   Operation timed out after 1 second")
    
    # 3. Graceful shutdown
    print("\n3. Graceful shutdown pattern:")
    
    class GracefulService:
        def __init__(self):
            self.running = True
            self.tasks = []
        
        async def run(self):
            """Main service loop."""
            while self.running:
                await asyncio.sleep(0.1)
                # Do work...
        
        async def shutdown(self):
            """Gracefully shutdown the service."""
            print("   Initiating graceful shutdown...")
            self.running = False
            
            # Cancel all tasks
            for task in self.tasks:
                task.cancel()
            
            # Wait for tasks to complete
            await asyncio.gather(*self.tasks, return_exceptions=True)
            print("   Shutdown complete")
    
    service = GracefulService()
    service_task = asyncio.create_task(service.run())
    service.tasks.append(service_task)
    
    # Run for a bit then shutdown
    await asyncio.sleep(0.5)
    await service.shutdown()

async def demonstrate_performance_tips():
    """Demonstrate performance optimization tips."""
    print(f"\n{'='*60}")
    print("ASYNC PERFORMANCE TIPS")
    print(f"{'='*60}")
    
    # 1. Avoid creating unnecessary tasks
    print("\n1. Task creation overhead:")
    
    # Bad: Creating task for each item
    start = time.perf_counter()
    tasks = []
    for i in range(1000):
        task = asyncio.create_task(asyncio.sleep(0))
        tasks.append(task)
    await asyncio.gather(*tasks)
    bad_time = time.perf_counter() - start
    
    # Good: Batch operations
    start = time.perf_counter()
    async def batch_sleep():
        for i in range(1000):
            await asyncio.sleep(0)
    await batch_sleep()
    good_time = time.perf_counter() - start
    
    print(f"   Creating 1000 tasks: {bad_time:.3f}s")
    print(f"   Single coroutine: {good_time:.3f}s")
    print(f"   Speedup: {bad_time/good_time:.1f}x")
    
    # 2. Use asyncio.gather vs as_completed
    print("\n2. gather vs as_completed:")
    
    async def variable_delay(n):
        delay = random.uniform(0.1, 0.5)
        await asyncio.sleep(delay)
        return n, delay
    
    # Using gather (waits for all)
    start = time.perf_counter()
    results = await asyncio.gather(*[variable_delay(i) for i in range(5)])
    gather_time = time.perf_counter() - start
    print(f"   gather (wait for all): {gather_time:.2f}s")
    
    # Using as_completed (process as ready)
    start = time.perf_counter()
    tasks = [asyncio.create_task(variable_delay(i)) for i in range(5)]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        # Can process result immediately
    completed_time = time.perf_counter() - start
    print(f"   as_completed (process as ready): {completed_time:.2f}s")

async def main():
    """Run all pattern demonstrations."""
    try:
        await demonstrate_patterns()
        await demonstrate_error_handling()
        await demonstrate_performance_tips()
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("Async Patterns and Best Practices")
    print(f"Python version: {sys.version}")
    print("=" * 60)
    
    # Run the main async function
    asyncio.run(main())
    
    print(f"\n{'='*60}")
    print("BEST PRACTICES SUMMARY")
    print(f"{'='*60}")
    print("1. Use exponential backoff for retries")
    print("2. Implement circuit breakers for fault tolerance")
    print("3. Rate limit external API calls")
    print("4. Handle exceptions with return_exceptions=True")
    print("5. Always set timeouts for external operations")
    print("6. Use connection pooling for efficiency")
    print("7. Implement graceful shutdown patterns")
    print("8. Monitor and log async operations")
    print("9. Avoid blocking the event loop")
    print("10. Batch operations when possible")
