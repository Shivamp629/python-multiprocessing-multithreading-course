#!/usr/bin/env python3
"""
FastAPI Internals Explorer

This script demonstrates how FastAPI handles sync and async endpoints,
explores the thread pool mechanics, and shows the performance implications
of different approaches.
"""

import asyncio
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import psutil
import httpx
from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
import anyio
import uvicorn
from typing import Dict, List
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)-10s - %(message)s'
)
logger = logging.getLogger(__name__)

# Track active threads
active_threads: Dict[int, str] = {}
thread_lock = threading.Lock()

def track_thread(operation: str):
    """Decorator to track which thread executes the function"""
    def decorator(func):
        def sync_wrapper(*args, **kwargs):
            thread_id = threading.get_ident()
            thread_name = threading.current_thread().name
            
            with thread_lock:
                active_threads[thread_id] = f"{operation} ({thread_name})"
                logger.info(f"Starting {operation} on thread {thread_id} ({thread_name})")
            
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                with thread_lock:
                    if thread_id in active_threads:
                        del active_threads[thread_id]
                logger.info(f"Completed {operation} on thread {thread_id}")
        
        async def async_wrapper(*args, **kwargs):
            thread_id = threading.get_ident()
            thread_name = threading.current_thread().name
            
            with thread_lock:
                active_threads[thread_id] = f"{operation} ({thread_name})"
                logger.info(f"Starting {operation} on thread {thread_id} ({thread_name})")
            
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                with thread_lock:
                    if thread_id in active_threads:
                        del active_threads[thread_id]
                logger.info(f"Completed {operation} on thread {thread_id}")
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


# Configure thread pool size
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Configure the thread pool size on startup"""
    # Get the current thread limiter
    limiter = anyio.to_thread.current_default_thread_limiter()
    logger.info(f"Default thread pool size: {limiter.total_tokens}")
    
    # You can modify it here
    # limiter.total_tokens = 100  # Increase from default 40
    
    yield
    
    logger.info("Shutting down...")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    """Root endpoint showing current system state"""
    process = psutil.Process()
    
    with thread_lock:
        current_threads = dict(active_threads)
    
    return {
        "thread_count": threading.active_count(),
        "active_threads": current_threads,
        "event_loop_thread": threading.current_thread().name,
        "process_threads": process.num_threads(),
        "cpu_percent": process.cpu_percent(),
        "memory_mb": process.memory_info().rss / 1024 / 1024
    }


@app.get("/sync-endpoint")
@track_thread("sync-endpoint")
def sync_endpoint():
    """Synchronous endpoint - runs in thread pool"""
    # Simulate blocking I/O
    time.sleep(1)
    
    return {
        "type": "sync",
        "thread_id": threading.get_ident(),
        "thread_name": threading.current_thread().name,
        "is_main_thread": threading.current_thread() is threading.main_thread()
    }


@app.get("/async-endpoint")
@track_thread("async-endpoint")
async def async_endpoint():
    """Asynchronous endpoint - runs on event loop"""
    # Simulate non-blocking I/O
    await asyncio.sleep(1)
    
    return {
        "type": "async",
        "thread_id": threading.get_ident(),
        "thread_name": threading.current_thread().name,
        "is_main_thread": threading.current_thread() is threading.main_thread()
    }


@app.get("/mixed-endpoint")
@track_thread("mixed-endpoint")
async def mixed_endpoint():
    """Mixed sync/async operations"""
    # Async operation
    await asyncio.sleep(0.5)
    
    # Run sync operation in thread pool
    def cpu_bound_task():
        total = 0
        for i in range(1000000):
            total += i
        return total
    
    # This runs the sync function in the thread pool
    result = await anyio.to_thread.run_sync(cpu_bound_task)
    
    return {
        "type": "mixed",
        "thread_id": threading.get_ident(),
        "thread_name": threading.current_thread().name,
        "cpu_result": result
    }


@app.get("/blocking-disaster")
@track_thread("blocking-disaster")
async def blocking_disaster():
    """WARNING: This blocks the event loop - DON'T DO THIS!"""
    # This is BAD - blocks the entire event loop
    time.sleep(2)  # Blocking call in async function!
    
    return {
        "type": "blocking_disaster",
        "warning": "This blocked the event loop for 2 seconds!",
        "thread_id": threading.get_ident()
    }


@app.get("/thread-pool-stress/{count}")
async def thread_pool_stress(count: int):
    """Stress test the thread pool with multiple sync operations"""
    if count > 100:
        return {"error": "Count too high, max 100"}
    
    start_time = time.time()
    
    async def run_sync_task(task_id: int):
        def sync_work():
            time.sleep(0.1)  # Simulate work
            return f"Task {task_id} completed"
        
        return await anyio.to_thread.run_sync(sync_work)
    
    # Run multiple sync tasks concurrently
    tasks = [run_sync_task(i) for i in range(count)]
    results = await asyncio.gather(*tasks)
    
    elapsed = time.time() - start_time
    
    return {
        "requested_tasks": count,
        "completed_tasks": len(results),
        "elapsed_time": elapsed,
        "tasks_per_second": count / elapsed,
        "thread_pool_info": {
            "active_threads": threading.active_count(),
            "limiter_tokens": anyio.to_thread.current_default_thread_limiter().borrowed_tokens
        }
    }


@app.get("/event-loop-stress/{count}")
async def event_loop_stress(count: int):
    """Stress test the event loop with multiple async operations"""
    if count > 10000:
        return {"error": "Count too high, max 10000"}
    
    start_time = time.time()
    
    async def async_work(task_id: int):
        await asyncio.sleep(0.01)  # Simulate async I/O
        return f"Task {task_id} completed"
    
    # Run multiple async tasks concurrently
    tasks = [async_work(i) for i in range(count)]
    results = await asyncio.gather(*tasks)
    
    elapsed = time.time() - start_time
    
    return {
        "requested_tasks": count,
        "completed_tasks": len(results),
        "elapsed_time": elapsed,
        "tasks_per_second": count / elapsed,
        "event_loop_thread": threading.current_thread().name
    }


@app.get("/compare-execution-models/{iterations}")
async def compare_execution_models(iterations: int = 10):
    """Compare different execution models"""
    results = {}
    
    # Test 1: Pure async
    start = time.time()
    async def async_task():
        await asyncio.sleep(0.01)
    
    await asyncio.gather(*[async_task() for _ in range(iterations)])
    results["pure_async"] = time.time() - start
    
    # Test 2: Sync in thread pool
    start = time.time()
    def sync_task():
        time.sleep(0.01)
    
    await asyncio.gather(*[
        anyio.to_thread.run_sync(sync_task) for _ in range(iterations)
    ])
    results["sync_in_thread_pool"] = time.time() - start
    
    # Test 3: CPU bound in thread pool
    start = time.time()
    def cpu_task():
        total = sum(range(100000))
        return total
    
    await asyncio.gather(*[
        anyio.to_thread.run_sync(cpu_task) for _ in range(iterations)
    ])
    results["cpu_in_thread_pool"] = time.time() - start
    
    return {
        "iterations": iterations,
        "execution_times": results,
        "winner": min(results, key=results.get),
        "thread_pool_size": anyio.to_thread.current_default_thread_limiter().total_tokens
    }


def demonstrate_thread_pool():
    """Standalone demonstration of thread pool behavior"""
    print("\n" + "="*60)
    print("Thread Pool Demonstration")
    print("="*60)
    
    # Show how ThreadPoolExecutor works (similar to FastAPI's internals)
    with ThreadPoolExecutor(max_workers=5) as executor:
        def worker(task_id):
            thread_name = threading.current_thread().name
            print(f"Task {task_id} running on {thread_name}")
            time.sleep(1)
            return f"Task {task_id} completed"
        
        # Submit multiple tasks
        futures = [executor.submit(worker, i) for i in range(10)]
        
        # Wait for all to complete
        for future in futures:
            result = future.result()
            print(result)
    
    print("\nThis is similar to how FastAPI handles sync endpoints!")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        demonstrate_thread_pool()
    else:
        print("\n" + "="*60)
        print("FastAPI Internals Explorer")
        print("="*60)
        print("\nStarting server on http://localhost:8000")
        print("\nTry these endpoints:")
        print("  - GET / - System state")
        print("  - GET /sync-endpoint - Runs in thread pool")
        print("  - GET /async-endpoint - Runs on event loop")
        print("  - GET /mixed-endpoint - Mix of sync and async")
        print("  - GET /blocking-disaster - DON'T DO THIS!")
        print("  - GET /thread-pool-stress/20 - Stress test thread pool")
        print("  - GET /event-loop-stress/100 - Stress test event loop")
        print("  - GET /compare-execution-models/10 - Performance comparison")
        print("\nWatch the logs to see which threads handle each request!")
        print("\nPress Ctrl+C to stop")
        print("="*60 + "\n")
        
        uvicorn.run(app, host="0.0.0.0", port=8000)
