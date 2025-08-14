#!/usr/bin/env python3
"""
Concurrent I/O Operations: Real-world async patterns

This script demonstrates how to handle concurrent I/O operations efficiently
using async/await, including HTTP requests, file I/O, and database operations.
"""

import asyncio
import aiohttp
import aiofiles
import time
import random
import json
import os
import sys
from datetime import datetime
from typing import List, Dict, Any
import sqlite3
from contextlib import asynccontextmanager

# Disable SSL warnings for demo
import ssl
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

class ConcurrentIODemo:
    """Demonstrates concurrent I/O operations with async/await."""
    
    def __init__(self):
        self.results = []
        self.start_time = None
    
    async def fetch_url(self, session: aiohttp.ClientSession, url: str, 
                       timeout: int = 5) -> Dict[str, Any]:
        """Fetch a URL asynchronously."""
        try:
            start = time.perf_counter()
            async with session.get(url, timeout=timeout, ssl=ssl_context) as response:
                data = await response.text()
                elapsed = time.perf_counter() - start
                
                return {
                    'url': url,
                    'status': response.status,
                    'length': len(data),
                    'time': elapsed,
                    'success': True
                }
        except asyncio.TimeoutError:
            return {'url': url, 'error': 'Timeout', 'success': False}
        except Exception as e:
            return {'url': url, 'error': str(e), 'success': False}
    
    async def demonstrate_concurrent_http(self):
        """Demonstrate concurrent HTTP requests."""
        print(f"\n{'='*60}")
        print("CONCURRENT HTTP REQUESTS")
        print(f"{'='*60}")
        
        # URLs to fetch
        urls = [
            'https://httpbin.org/delay/1',
            'https://httpbin.org/delay/2',
            'https://httpbin.org/delay/1',
            'https://jsonplaceholder.typicode.com/posts/1',
            'https://jsonplaceholder.typicode.com/posts/2',
            'https://api.github.com/users/python',
        ]
        
        # Sequential approach
        print("\n1. Sequential HTTP requests:")
        start = time.perf_counter()
        
        async with aiohttp.ClientSession() as session:
            sequential_results = []
            for url in urls:
                result = await self.fetch_url(session, url)
                sequential_results.append(result)
                print(f"   Fetched {url}: {result['time']:.2f}s")
        
        sequential_time = time.perf_counter() - start
        print(f"   Total time: {sequential_time:.2f}s")
        
        # Concurrent approach
        print("\n2. Concurrent HTTP requests:")
        start = time.perf_counter()
        
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_url(session, url) for url in urls]
            concurrent_results = await asyncio.gather(*tasks)
            
            for result in concurrent_results:
                if result['success']:
                    print(f"   Fetched {result['url']}: {result['time']:.2f}s")
                else:
                    print(f"   Failed {result['url']}: {result['error']}")
        
        concurrent_time = time.perf_counter() - start
        print(f"   Total time: {concurrent_time:.2f}s")
        print(f"   Speedup: {sequential_time/concurrent_time:.1f}x")
        
        # Limited concurrency
        print("\n3. Limited concurrency (max 3 concurrent):")
        
        async def fetch_with_semaphore(session, url, semaphore):
            async with semaphore:
                return await self.fetch_url(session, url)
        
        start = time.perf_counter()
        semaphore = asyncio.Semaphore(3)
        
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_with_semaphore(session, url, semaphore) for url in urls]
            limited_results = await asyncio.gather(*tasks)
        
        limited_time = time.perf_counter() - start
        print(f"   Total time: {limited_time:.2f}s")
        print("   ✓ Semaphore limits concurrent connections")
    
    async def demonstrate_file_io(self):
        """Demonstrate async file I/O operations."""
        print(f"\n{'='*60}")
        print("ASYNC FILE I/O")
        print(f"{'='*60}")
        
        # Create test directory
        test_dir = "async_test_files"
        os.makedirs(test_dir, exist_ok=True)
        
        # Generate test data
        files = [f"{test_dir}/file_{i}.txt" for i in range(10)]
        data = [f"This is test data for file {i}\n" * 100 for i in range(10)]
        
        # Async write files
        print("\n1. Writing files concurrently:")
        start = time.perf_counter()
        
        async def write_file(filename, content):
            async with aiofiles.open(filename, 'w') as f:
                await f.write(content)
                return filename
        
        write_tasks = [write_file(f, d) for f, d in zip(files, data)]
        await asyncio.gather(*write_tasks)
        
        write_time = time.perf_counter() - start
        print(f"   Wrote {len(files)} files in {write_time:.3f}s")
        
        # Async read files
        print("\n2. Reading files concurrently:")
        start = time.perf_counter()
        
        async def read_file(filename):
            async with aiofiles.open(filename, 'r') as f:
                content = await f.read()
                return len(content)
        
        read_tasks = [read_file(f) for f in files]
        sizes = await asyncio.gather(*read_tasks)
        
        read_time = time.perf_counter() - start
        print(f"   Read {len(files)} files in {read_time:.3f}s")
        print(f"   Total size: {sum(sizes)} bytes")
        
        # Cleanup
        for f in files:
            os.remove(f)
        os.rmdir(test_dir)
        
        # Compare with sync I/O
        print("\n3. Comparison with synchronous I/O:")
        print(f"   Async write: {write_time:.3f}s")
        print(f"   Async read: {read_time:.3f}s")
        print("   Note: File I/O benefits less from async than network I/O")
    
    async def demonstrate_database_operations(self):
        """Demonstrate async database operations."""
        print(f"\n{'='*60}")
        print("ASYNC DATABASE OPERATIONS")
        print(f"{'='*60}")
        
        # Note: Using sqlite3 with run_in_executor for demo
        # In production, use proper async database drivers like asyncpg
        
        db_file = "async_test.db"
        
        # Database operations wrapper
        async def run_db_operation(func, *args):
            """Run database operation in thread pool."""
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, func, *args)
        
        # Setup database
        def setup_db():
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    created_at TIMESTAMP
                )
            ''')
            conn.commit()
            conn.close()
        
        await run_db_operation(setup_db)
        
        # Insert operations
        print("\n1. Concurrent insert operations:")
        
        def insert_user(user_id, name, email):
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)",
                (user_id, name, email, datetime.now())
            )
            conn.commit()
            conn.close()
            return user_id
        
        start = time.perf_counter()
        
        insert_tasks = []
        for i in range(50):
            task = run_db_operation(
                insert_user, i, f"User{i}", f"user{i}@example.com"
            )
            insert_tasks.append(task)
        
        await asyncio.gather(*insert_tasks)
        
        insert_time = time.perf_counter() - start
        print(f"   Inserted 50 records in {insert_time:.3f}s")
        
        # Query operations
        print("\n2. Concurrent query operations:")
        
        def query_user(user_id):
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
            result = cursor.fetchone()
            conn.close()
            return result
        
        start = time.perf_counter()
        
        query_tasks = []
        for i in range(0, 50, 5):
            task = run_db_operation(query_user, i)
            query_tasks.append(task)
        
        results = await asyncio.gather(*query_tasks)
        
        query_time = time.perf_counter() - start
        print(f"   Queried {len(results)} records in {query_time:.3f}s")
        
        # Cleanup
        os.remove(db_file)
        
        print("\n✓ Use proper async database drivers in production")
        print("✓ Examples: asyncpg (PostgreSQL), aiomysql (MySQL)")
    
    async def demonstrate_mixed_io_patterns(self):
        """Demonstrate mixed I/O patterns and coordination."""
        print(f"\n{'='*60}")
        print("MIXED I/O PATTERNS")
        print(f"{'='*60}")
        
        # Pattern 1: Fan-out/Fan-in
        print("\n1. Fan-out/Fan-in pattern:")
        
        async def process_item(item_id):
            """Process single item with multiple I/O operations."""
            results = {}
            
            # Simulate different I/O operations
            async def fetch_from_api():
                await asyncio.sleep(random.uniform(0.1, 0.5))
                return f"API data for {item_id}"
            
            async def read_from_cache():
                await asyncio.sleep(random.uniform(0.05, 0.1))
                return f"Cache data for {item_id}"
            
            async def query_database():
                await asyncio.sleep(random.uniform(0.2, 0.3))
                return f"DB data for {item_id}"
            
            # Fan-out: Execute all operations concurrently
            api_task = asyncio.create_task(fetch_from_api())
            cache_task = asyncio.create_task(read_from_cache())
            db_task = asyncio.create_task(query_database())
            
            # Fan-in: Collect all results
            results['api'] = await api_task
            results['cache'] = await cache_task
            results['db'] = await db_task
            
            return item_id, results
        
        start = time.perf_counter()
        items = [1, 2, 3, 4, 5]
        
        # Process all items concurrently
        item_results = await asyncio.gather(*[process_item(i) for i in items])
        
        elapsed = time.perf_counter() - start
        print(f"   Processed {len(items)} items in {elapsed:.2f}s")
        for item_id, results in item_results[:2]:  # Show first 2
            print(f"   Item {item_id}: {list(results.keys())}")
        
        # Pattern 2: Pipeline pattern
        print("\n2. Pipeline pattern:")
        
        async def stage1_fetch(item):
            """Stage 1: Fetch raw data."""
            await asyncio.sleep(0.1)
            return {'item': item, 'raw_data': f"raw_{item}"}
        
        async def stage2_process(data):
            """Stage 2: Process data."""
            await asyncio.sleep(0.15)
            data['processed'] = f"processed_{data['raw_data']}"
            return data
        
        async def stage3_store(data):
            """Stage 3: Store results."""
            await asyncio.sleep(0.05)
            data['stored'] = True
            return data
        
        async def pipeline(item):
            """Run item through pipeline."""
            data = await stage1_fetch(item)
            data = await stage2_process(data)
            data = await stage3_store(data)
            return data
        
        start = time.perf_counter()
        
        # Process items through pipeline
        pipeline_results = await asyncio.gather(*[pipeline(i) for i in range(5)])
        
        elapsed = time.perf_counter() - start
        print(f"   Pipeline processed {len(pipeline_results)} items in {elapsed:.2f}s")
        
        # Pattern 3: Timeout and retry pattern
        print("\n3. Timeout and retry pattern:")
        
        async def unreliable_operation(success_rate=0.7):
            """Operation that might fail or timeout."""
            await asyncio.sleep(random.uniform(0.1, 2.0))
            if random.random() < success_rate:
                return "Success"
            else:
                raise Exception("Operation failed")
        
        async def with_timeout_retry(operation, timeout=1.0, retries=3):
            """Execute operation with timeout and retry logic."""
            for attempt in range(retries):
                try:
                    result = await asyncio.wait_for(operation(), timeout=timeout)
                    return result, attempt + 1
                except asyncio.TimeoutError:
                    print(f"     Attempt {attempt + 1}: Timeout")
                except Exception as e:
                    print(f"     Attempt {attempt + 1}: {e}")
                
                if attempt < retries - 1:
                    await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
            
            return None, retries
        
        results = await asyncio.gather(
            with_timeout_retry(unreliable_operation),
            with_timeout_retry(unreliable_operation),
            with_timeout_retry(unreliable_operation),
        )
        
        success_count = sum(1 for r, _ in results if r is not None)
        print(f"   Success rate: {success_count}/3")
    
    async def demonstrate_performance_monitoring(self):
        """Monitor and measure async performance."""
        print(f"\n{'='*60}")
        print("PERFORMANCE MONITORING")
        print(f"{'='*60}")
        
        # Custom context manager for timing
        @asynccontextmanager
        async def timed_operation(name):
            start = time.perf_counter()
            try:
                yield
            finally:
                elapsed = time.perf_counter() - start
                print(f"   {name}: {elapsed:.3f}s")
        
        # Monitor concurrent operations
        print("\n1. Operation timing:")
        
        async def monitored_operation(op_id, duration):
            async with timed_operation(f"Operation {op_id}"):
                await asyncio.sleep(duration)
                return op_id
        
        await asyncio.gather(
            monitored_operation(1, 0.5),
            monitored_operation(2, 0.3),
            monitored_operation(3, 0.7)
        )
        
        # Monitor event loop lag
        print("\n2. Event loop responsiveness:")
        
        lag_measurements = []
        
        async def measure_lag():
            for _ in range(10):
                start = time.perf_counter()
                await asyncio.sleep(0)  # Yield to event loop
                lag = (time.perf_counter() - start) * 1000  # Convert to ms
                lag_measurements.append(lag)
                await asyncio.sleep(0.1)
        
        await measure_lag()
        
        avg_lag = sum(lag_measurements) / len(lag_measurements)
        max_lag = max(lag_measurements)
        print(f"   Average lag: {avg_lag:.3f} ms")
        print(f"   Maximum lag: {max_lag:.3f} ms")
        
        # Task count monitoring
        print("\n3. Active tasks monitoring:")
        
        async def monitor_tasks():
            initial_tasks = len(asyncio.all_tasks())
            print(f"   Initial tasks: {initial_tasks}")
            
            # Create some tasks
            tasks = [asyncio.create_task(asyncio.sleep(i * 0.1)) for i in range(5)]
            
            peak_tasks = len(asyncio.all_tasks())
            print(f"   Peak tasks: {peak_tasks}")
            
            # Wait for completion
            await asyncio.gather(*tasks)
            
            final_tasks = len(asyncio.all_tasks())
            print(f"   Final tasks: {final_tasks}")
        
        await monitor_tasks()

async def demonstrate_real_world_example():
    """A real-world example: Web scraper with rate limiting."""
    print(f"\n{'='*60}")
    print("REAL-WORLD EXAMPLE: ASYNC WEB SCRAPER")
    print(f"{'='*60}")
    
    class AsyncWebScraper:
        def __init__(self, rate_limit=5, timeout=10):
            self.rate_limit = rate_limit
            self.timeout = timeout
            self.semaphore = asyncio.Semaphore(rate_limit)
            self.session = None
        
        async def __aenter__(self):
            self.session = aiohttp.ClientSession()
            return self
        
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            await self.session.close()
        
        async def fetch_page(self, url):
            """Fetch a single page with rate limiting."""
            async with self.semaphore:
                try:
                    async with self.session.get(
                        url, timeout=self.timeout, ssl=ssl_context
                    ) as response:
                        return {
                            'url': url,
                            'status': response.status,
                            'content': await response.text(),
                            'headers': dict(response.headers)
                        }
                except Exception as e:
                    return {'url': url, 'error': str(e)}
        
        async def scrape_urls(self, urls):
            """Scrape multiple URLs concurrently."""
            tasks = [self.fetch_page(url) for url in urls]
            return await asyncio.gather(*tasks)
    
    # Example usage
    urls = [
        'https://httpbin.org/html',
        'https://httpbin.org/json',
        'https://httpbin.org/xml',
        'https://httpbin.org/robots.txt',
        'https://httpbin.org/forms/post',
    ]
    
    print(f"\nScraping {len(urls)} URLs with rate limit of 5 concurrent requests...")
    
    start = time.perf_counter()
    
    async with AsyncWebScraper(rate_limit=3) as scraper:
        results = await scraper.scrape_urls(urls)
        
        for result in results:
            if 'error' in result:
                print(f"   Failed: {result['url']} - {result['error']}")
            else:
                print(f"   Success: {result['url']} - "
                      f"{result['status']} - {len(result['content'])} bytes")
    
    elapsed = time.perf_counter() - start
    print(f"\nCompleted in {elapsed:.2f}s")
    print("✓ Rate limiting prevents overwhelming the server")
    print("✓ Async context manager ensures proper cleanup")

async def main():
    """Run all concurrent I/O demonstrations."""
    demo = ConcurrentIODemo()
    
    try:
        await demo.demonstrate_concurrent_http()
        await demo.demonstrate_file_io()
        await demo.demonstrate_database_operations()
        await demo.demonstrate_mixed_io_patterns()
        await demo.demonstrate_performance_monitoring()
        await demonstrate_real_world_example()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("Concurrent I/O Operations Demonstration")
    print(f"Python version: {sys.version}")
    print("=" * 60)
    
    # Run the main async function
    asyncio.run(main())
    
    print(f"\n{'='*60}")
    print("KEY INSIGHTS")
    print(f"{'='*60}")
    print("1. Async excels at concurrent I/O operations")
    print("2. Use aiohttp for HTTP, aiofiles for files")
    print("3. Semaphores control concurrency levels")
    print("4. gather() runs tasks concurrently")
    print("5. Always use async context managers for cleanup")
    print("6. Monitor performance to find bottlenecks")
