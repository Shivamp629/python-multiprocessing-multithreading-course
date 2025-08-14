#!/usr/bin/env python3
"""
FastAPI Performance Comparison: Sync vs Async

This script benchmarks different endpoint patterns to demonstrate
the performance implications of various implementation choices.
"""

import asyncio
import time
import random
import statistics
from typing import List, Dict, Any
import httpx
import psutil
import threading
from datetime import datetime
import json

from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse
import uvicorn
import aiofiles
import databases
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float

# Mock database setup
DATABASE_URL = "sqlite:///./test.db"
database = databases.Database(DATABASE_URL)
metadata = MetaData()

# Create a simple table for testing
items_table = Table(
    "items",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(50)),
    Column("value", Float),
)

# Sync database engine for comparison
sync_engine = create_engine(DATABASE_URL)
metadata.create_all(sync_engine)

app = FastAPI()


@app.on_event("startup")
async def startup():
    """Initialize database connection"""
    await database.connect()
    
    # Insert some test data if table is empty
    count = await database.fetch_val("SELECT COUNT(*) FROM items")
    if count == 0:
        for i in range(100):
            await database.execute(
                "INSERT INTO items (name, value) VALUES (:name, :value)",
                {"name": f"Item {i}", "value": random.random() * 100}
            )


@app.on_event("shutdown")
async def shutdown():
    """Close database connection"""
    await database.disconnect()


# === COMPARISON 1: Basic I/O Operations ===

@app.get("/sync/sleep/{duration}")
def sync_sleep(duration: float = 1.0):
    """Synchronous sleep - blocks thread"""
    start = time.time()
    time.sleep(duration)
    return {
        "type": "sync_sleep",
        "duration": duration,
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


@app.get("/async/sleep/{duration}")
async def async_sleep(duration: float = 1.0):
    """Asynchronous sleep - yields to event loop"""
    start = time.time()
    await asyncio.sleep(duration)
    return {
        "type": "async_sleep",
        "duration": duration,
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


@app.get("/async/sleep-wrong/{duration}")
async def async_sleep_wrong(duration: float = 1.0):
    """WRONG: Blocking call in async function"""
    start = time.time()
    time.sleep(duration)  # This blocks the event loop!
    return {
        "type": "async_sleep_wrong",
        "duration": duration,
        "warning": "This blocked the event loop!",
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


# === COMPARISON 2: Database Operations ===

@app.get("/sync/database")
def sync_database_query():
    """Synchronous database query"""
    start = time.time()
    
    with sync_engine.connect() as conn:
        result = conn.execute("SELECT * FROM items WHERE value > 50")
        items = [dict(row) for row in result]
    
    return {
        "type": "sync_database",
        "count": len(items),
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


@app.get("/async/database")
async def async_database_query():
    """Asynchronous database query"""
    start = time.time()
    
    query = "SELECT * FROM items WHERE value > 50"
    rows = await database.fetch_all(query)
    
    return {
        "type": "async_database",
        "count": len(rows),
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


# === COMPARISON 3: HTTP Requests ===

@app.get("/sync/http-request")
def sync_http_request():
    """Synchronous HTTP request"""
    import requests
    
    start = time.time()
    response = requests.get("https://httpbin.org/delay/1")
    
    return {
        "type": "sync_http",
        "status": response.status_code,
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


@app.get("/async/http-request")
async def async_http_request():
    """Asynchronous HTTP request"""
    start = time.time()
    
    async with httpx.AsyncClient() as client:
        response = await client.get("https://httpbin.org/delay/1")
    
    return {
        "type": "async_http",
        "status": response.status_code,
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


# === COMPARISON 4: CPU-Bound Operations ===

def cpu_bound_task(n: int = 1000000):
    """CPU-intensive calculation"""
    total = 0
    for i in range(n):
        total += i * i
    return total


@app.get("/sync/cpu-bound")
def sync_cpu_bound(n: int = 1000000):
    """Synchronous CPU-bound operation"""
    start = time.time()
    result = cpu_bound_task(n)
    
    return {
        "type": "sync_cpu",
        "result": result,
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


@app.get("/async/cpu-bound")
async def async_cpu_bound(n: int = 1000000):
    """Async CPU-bound - still blocks event loop!"""
    start = time.time()
    result = cpu_bound_task(n)  # This blocks!
    
    return {
        "type": "async_cpu_wrong",
        "result": result,
        "warning": "CPU-bound in async still blocks!",
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


@app.get("/async/cpu-bound-correct")
async def async_cpu_bound_correct(n: int = 1000000):
    """Correct way: CPU-bound in thread pool"""
    import anyio
    
    start = time.time()
    result = await anyio.to_thread.run_sync(cpu_bound_task, n)
    
    return {
        "type": "async_cpu_thread_pool",
        "result": result,
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


# === COMPARISON 5: File Operations ===

@app.get("/sync/file-read")
def sync_file_read():
    """Synchronous file read"""
    start = time.time()
    
    with open(__file__, 'r') as f:
        content = f.read()
    
    return {
        "type": "sync_file",
        "size": len(content),
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


@app.get("/async/file-read")
async def async_file_read():
    """Asynchronous file read"""
    start = time.time()
    
    async with aiofiles.open(__file__, 'r') as f:
        content = await f.read()
    
    return {
        "type": "async_file",
        "size": len(content),
        "thread": threading.current_thread().name,
        "elapsed": time.time() - start
    }


# === BENCHMARK ENDPOINTS ===

@app.get("/benchmark/concurrency/{endpoint}")
async def benchmark_concurrency(
    endpoint: str,
    requests: int = Query(default=10, le=100),
    concurrent: int = Query(default=5, le=50)
):
    """Benchmark an endpoint with concurrent requests"""
    
    base_url = "http://localhost:8002"
    
    # Map of endpoint names to URLs
    endpoints = {
        "sync_sleep": f"{base_url}/sync/sleep/0.5",
        "async_sleep": f"{base_url}/async/sleep/0.5",
        "async_sleep_wrong": f"{base_url}/async/sleep-wrong/0.5",
        "sync_db": f"{base_url}/sync/database",
        "async_db": f"{base_url}/async/database",
        "sync_http": f"{base_url}/sync/http-request",
        "async_http": f"{base_url}/async/http-request",
    }
    
    if endpoint not in endpoints:
        return {"error": f"Unknown endpoint. Choose from: {list(endpoints.keys())}"}
    
    url = endpoints[endpoint]
    
    async def make_request(client: httpx.AsyncClient) -> float:
        start = time.time()
        try:
            response = await client.get(url, timeout=30.0)
            response.raise_for_status()
            return time.time() - start
        except Exception as e:
            print(f"Request failed: {e}")
            return -1
    
    # Run benchmark
    start_time = time.time()
    times: List[float] = []
    
    async with httpx.AsyncClient() as client:
        # Run requests in batches
        for i in range(0, requests, concurrent):
            batch = min(concurrent, requests - i)
            tasks = [make_request(client) for _ in range(batch)]
            batch_times = await asyncio.gather(*tasks)
            times.extend([t for t in batch_times if t > 0])
    
    total_time = time.time() - start_time
    
    # Calculate statistics
    if times:
        return {
            "endpoint": endpoint,
            "url": url,
            "total_requests": requests,
            "successful_requests": len(times),
            "concurrent_requests": concurrent,
            "total_time": total_time,
            "requests_per_second": len(times) / total_time,
            "latency": {
                "min": min(times),
                "max": max(times),
                "mean": statistics.mean(times),
                "median": statistics.median(times),
                "p95": statistics.quantiles(times, n=20)[18] if len(times) > 20 else max(times),
                "p99": statistics.quantiles(times, n=100)[98] if len(times) > 100 else max(times),
            }
        }
    else:
        return {"error": "All requests failed"}


@app.get("/benchmark/comparison")
async def benchmark_comparison():
    """Run a comprehensive comparison of sync vs async patterns"""
    
    results = {}
    base_url = "http://localhost:8002"
    
    # Test configurations
    tests = [
        {
            "name": "I/O Bound (Sleep)",
            "sync_url": f"{base_url}/sync/sleep/0.1",
            "async_url": f"{base_url}/async/sleep/0.1",
            "requests": 20,
            "concurrent": 10
        },
        {
            "name": "Database Query",
            "sync_url": f"{base_url}/sync/database",
            "async_url": f"{base_url}/async/database",
            "requests": 20,
            "concurrent": 10
        },
        {
            "name": "HTTP Request",
            "sync_url": f"{base_url}/sync/http-request",
            "async_url": f"{base_url}/async/http-request",
            "requests": 10,
            "concurrent": 5
        }
    ]
    
    async def run_test(url: str, requests: int, concurrent: int) -> Dict[str, Any]:
        times = []
        start = time.time()
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            for i in range(0, requests, concurrent):
                batch = min(concurrent, requests - i)
                tasks = []
                for _ in range(batch):
                    task_start = time.time()
                    tasks.append(client.get(url))
                
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                
                for response in responses:
                    if isinstance(response, httpx.Response):
                        times.append(time.time() - task_start)
        
        total_time = time.time() - start
        
        return {
            "total_time": total_time,
            "requests_per_second": len(times) / total_time if times else 0,
            "successful_requests": len(times),
            "average_latency": statistics.mean(times) if times else 0
        }
    
    # Run tests
    for test in tests:
        sync_result = await run_test(test["sync_url"], test["requests"], test["concurrent"])
        async_result = await run_test(test["async_url"], test["requests"], test["concurrent"])
        
        results[test["name"]] = {
            "sync": sync_result,
            "async": async_result,
            "speedup": async_result["requests_per_second"] / sync_result["requests_per_second"] 
                      if sync_result["requests_per_second"] > 0 else 0
        }
    
    return results


@app.get("/system/status")
async def system_status():
    """Get current system status"""
    process = psutil.Process()
    
    return {
        "timestamp": datetime.now().isoformat(),
        "process": {
            "cpu_percent": process.cpu_percent(),
            "memory_mb": process.memory_info().rss / 1024 / 1024,
            "threads": process.num_threads(),
            "connections": len(process.connections())
        },
        "system": {
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_percent": psutil.virtual_memory().percent,
            "active_threads": threading.active_count()
        }
    }


# === STREAMING COMPARISON ===

@app.get("/sync/stream")
def sync_stream():
    """Synchronous streaming - blocks between chunks"""
    def generate():
        for i in range(10):
            time.sleep(0.5)  # Simulate slow generation
            yield f"Chunk {i}\n"
    
    return StreamingResponse(generate(), media_type="text/plain")


@app.get("/async/stream")
async def async_stream():
    """Asynchronous streaming - non-blocking"""
    async def generate():
        for i in range(10):
            await asyncio.sleep(0.5)  # Non-blocking
            yield f"Chunk {i}\n"
    
    return StreamingResponse(generate(), media_type="text/plain")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("FastAPI Performance Comparison: Sync vs Async")
    print("="*60)
    print("\nStarting server on http://localhost:8002")
    print("\nComparison endpoints:")
    print("\n1. Basic I/O:")
    print("   - GET /sync/sleep/{duration}")
    print("   - GET /async/sleep/{duration}")
    print("   - GET /async/sleep-wrong/{duration} (blocking in async)")
    print("\n2. Database:")
    print("   - GET /sync/database")
    print("   - GET /async/database")
    print("\n3. HTTP Requests:")
    print("   - GET /sync/http-request")
    print("   - GET /async/http-request")
    print("\n4. CPU-Bound:")
    print("   - GET /sync/cpu-bound")
    print("   - GET /async/cpu-bound (wrong)")
    print("   - GET /async/cpu-bound-correct")
    print("\n5. File Operations:")
    print("   - GET /sync/file-read")
    print("   - GET /async/file-read")
    print("\nBenchmark endpoints:")
    print("   - GET /benchmark/concurrency/{endpoint}")
    print("   - GET /benchmark/comparison")
    print("   - GET /system/status")
    print("\nStreaming:")
    print("   - GET /sync/stream")
    print("   - GET /async/stream")
    print("\nTry the benchmark comparison to see the performance difference!")
    print("\nPress Ctrl+C to stop")
    print("="*60 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8002)
