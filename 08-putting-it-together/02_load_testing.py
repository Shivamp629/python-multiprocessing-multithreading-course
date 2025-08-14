#!/usr/bin/env python3
"""
Load Testing and Benchmarking Tools

This script provides tools to test and compare the performance of different
server implementations under various workloads.
"""

import asyncio
import aiohttp
import time
import threading
import multiprocessing as mp
import requests
import statistics
import json
import sys
import matplotlib.pyplot as plt
from datetime import datetime
from collections import defaultdict
import numpy as np

class LoadTester:
    """Base class for load testing."""
    
    def __init__(self, base_url="http://127.0.0.1:8080"):
        self.base_url = base_url
        self.results = {
            'response_times': [],
            'errors': 0,
            'status_codes': defaultdict(int),
            'bytes_received': 0,
            'start_time': None,
            'end_time': None
        }
    
    def get_stats(self):
        """Calculate statistics from results."""
        times = self.results['response_times']
        if not times:
            return None
        
        duration = self.results['end_time'] - self.results['start_time']
        successful = len(times)
        total_requests = successful + self.results['errors']
        
        return {
            'total_requests': total_requests,
            'successful_requests': successful,
            'failed_requests': self.results['errors'],
            'duration': duration,
            'requests_per_second': total_requests / duration if duration > 0 else 0,
            'min_time': min(times) * 1000,  # Convert to ms
            'max_time': max(times) * 1000,
            'mean_time': statistics.mean(times) * 1000,
            'median_time': statistics.median(times) * 1000,
            'p95_time': np.percentile(times, 95) * 1000,
            'p99_time': np.percentile(times, 99) * 1000,
            'bytes_received': self.results['bytes_received'],
            'status_codes': dict(self.results['status_codes'])
        }

class SynchronousLoadTester(LoadTester):
    """Synchronous load tester using threads."""
    
    def run_test(self, num_requests=1000, num_threads=10, endpoint='/'):
        """Run load test with threads."""
        print(f"\nRunning synchronous load test:")
        print(f"  Requests: {num_requests}")
        print(f"  Threads: {num_threads}")
        print(f"  Endpoint: {endpoint}")
        
        # Reset results
        self.results = {
            'response_times': [],
            'errors': 0,
            'status_codes': defaultdict(int),
            'bytes_received': 0,
            'start_time': time.time(),
            'end_time': None
        }
        
        # Thread-safe result collection
        lock = threading.Lock()
        
        def worker(num_requests_per_thread):
            """Worker thread function."""
            session = requests.Session()
            
            for _ in range(num_requests_per_thread):
                try:
                    start = time.time()
                    response = session.get(self.base_url + endpoint, timeout=5)
                    elapsed = time.time() - start
                    
                    with lock:
                        self.results['response_times'].append(elapsed)
                        self.results['status_codes'][response.status_code] += 1
                        self.results['bytes_received'] += len(response.content)
                        
                except Exception as e:
                    with lock:
                        self.results['errors'] += 1
            
            session.close()
        
        # Distribute requests among threads
        requests_per_thread = num_requests // num_threads
        remaining = num_requests % num_threads
        
        threads = []
        for i in range(num_threads):
            thread_requests = requests_per_thread + (1 if i < remaining else 0)
            t = threading.Thread(target=worker, args=(thread_requests,))
            threads.append(t)
            t.start()
        
        # Wait for completion
        for t in threads:
            t.join()
        
        self.results['end_time'] = time.time()
        
        return self.get_stats()

class AsynchronousLoadTester(LoadTester):
    """Asynchronous load tester using asyncio."""
    
    async def fetch(self, session, url):
        """Fetch a single URL."""
        try:
            start = time.time()
            async with session.get(url) as response:
                content = await response.read()
                elapsed = time.time() - start
                
                self.results['response_times'].append(elapsed)
                self.results['status_codes'][response.status] += 1
                self.results['bytes_received'] += len(content)
                
        except Exception as e:
            self.results['errors'] += 1
    
    async def run_test_async(self, num_requests=1000, num_concurrent=100, endpoint='/'):
        """Run async load test."""
        print(f"\nRunning asynchronous load test:")
        print(f"  Requests: {num_requests}")
        print(f"  Concurrent: {num_concurrent}")
        print(f"  Endpoint: {endpoint}")
        
        # Reset results
        self.results = {
            'response_times': [],
            'errors': 0,
            'status_codes': defaultdict(int),
            'bytes_received': 0,
            'start_time': time.time(),
            'end_time': None
        }
        
        url = self.base_url + endpoint
        
        # Create session with connection limit
        connector = aiohttp.TCPConnector(limit=num_concurrent)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Create tasks
            tasks = []
            for _ in range(num_requests):
                task = asyncio.create_task(self.fetch(session, url))
                tasks.append(task)
            
            # Run with progress reporting
            completed = 0
            for coro in asyncio.as_completed(tasks):
                await coro
                completed += 1
                if completed % 100 == 0:
                    print(f"  Progress: {completed}/{num_requests}")
        
        self.results['end_time'] = time.time()
        
        return self.get_stats()
    
    def run_test(self, num_requests=1000, num_concurrent=100, endpoint='/'):
        """Wrapper to run async test."""
        return asyncio.run(self.run_test_async(num_requests, num_concurrent, endpoint))

class WorkloadSimulator:
    """Simulate different types of workloads."""
    
    def __init__(self, base_url="http://127.0.0.1:8080"):
        self.base_url = base_url
        self.scenarios = {
            'cpu_bound': {
                'endpoint': '/compute',
                'description': 'CPU-intensive workload'
            },
            'io_bound': {
                'endpoint': '/io',
                'description': 'I/O-intensive workload'
            },
            'mixed': {
                'endpoints': ['/compute', '/io', '/'],
                'description': 'Mixed workload'
            },
            'static': {
                'endpoint': '/',
                'description': 'Static content'
            }
        }
    
    async def simulate_real_world_pattern(self, duration=60):
        """Simulate realistic traffic patterns."""
        print(f"\nSimulating real-world traffic pattern for {duration} seconds...")
        
        start_time = time.time()
        results = []
        
        # Traffic pattern: varying load over time
        async def generate_load(rate):
            """Generate requests at specified rate."""
            interval = 1.0 / rate if rate > 0 else 1.0
            
            async with aiohttp.ClientSession() as session:
                while time.time() - start_time < duration:
                    # Pick random endpoint
                    endpoint = np.random.choice(
                        ['/', '/compute', '/io'],
                        p=[0.7, 0.2, 0.1]  # 70% static, 20% CPU, 10% I/O
                    )
                    
                    try:
                        start = time.time()
                        async with session.get(self.base_url + endpoint) as resp:
                            await resp.read()
                            elapsed = time.time() - start
                            
                            results.append({
                                'timestamp': time.time() - start_time,
                                'endpoint': endpoint,
                                'response_time': elapsed,
                                'status': resp.status
                            })
                    except:
                        pass
                    
                    await asyncio.sleep(interval)
        
        # Simulate varying load (sine wave pattern)
        tasks = []
        base_rate = 50  # Base requests per second
        
        for i in range(10):  # 10 concurrent load generators
            # Vary rate over time
            phase = i * 2 * np.pi / 10
            rate_func = lambda t: base_rate * (1 + 0.5 * np.sin(2 * np.pi * t / 30 + phase))
            
            task = asyncio.create_task(generate_load(base_rate))
            tasks.append(task)
        
        # Wait for duration
        await asyncio.sleep(duration)
        
        # Cancel tasks
        for task in tasks:
            task.cancel()
        
        return results

def compare_server_performance():
    """Compare performance of different server implementations."""
    print("\n" + "="*60)
    print("SERVER PERFORMANCE COMPARISON")
    print("="*60)
    
    # Test configuration
    test_configs = [
        {'name': 'Light Load', 'requests': 1000, 'concurrent': 10},
        {'name': 'Medium Load', 'requests': 5000, 'concurrent': 50},
        {'name': 'Heavy Load', 'requests': 10000, 'concurrent': 200},
    ]
    
    results = defaultdict(dict)
    
    # Note: This assumes servers are running on different ports
    servers = {
        'Threaded': 'http://127.0.0.1:8080',
        'Multiprocess': 'http://127.0.0.1:8081',
        'Async': 'http://127.0.0.1:8082'
    }
    
    # Test each server
    for server_name, base_url in servers.items():
        print(f"\nTesting {server_name} server...")
        
        # Check if server is running
        try:
            response = requests.get(base_url, timeout=1)
            if response.status_code != 200:
                print(f"  Server not responding properly")
                continue
        except:
            print(f"  Server not running at {base_url}")
            continue
        
        tester = AsynchronousLoadTester(base_url)
        
        for config in test_configs:
            print(f"\n  {config['name']}:")
            stats = tester.run_test(
                num_requests=config['requests'],
                num_concurrent=config['concurrent']
            )
            
            if stats:
                results[server_name][config['name']] = stats
                print(f"    RPS: {stats['requests_per_second']:.1f}")
                print(f"    Mean latency: {stats['mean_time']:.1f} ms")
                print(f"    P95 latency: {stats['p95_time']:.1f} ms")
    
    return results

def visualize_results(results):
    """Create visualization of benchmark results."""
    if not results:
        print("No results to visualize")
        return
    
    # Prepare data
    servers = list(results.keys())
    load_types = ['Light Load', 'Medium Load', 'Heavy Load']
    
    # Create subplots
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle('Server Performance Comparison', fontsize=16)
    
    # 1. Requests per second
    ax = axes[0, 0]
    for load_type in load_types:
        rps_values = []
        for server in servers:
            if load_type in results[server]:
                rps_values.append(results[server][load_type]['requests_per_second'])
            else:
                rps_values.append(0)
        
        x = np.arange(len(servers))
        ax.bar(x + load_types.index(load_type) * 0.25, rps_values, 0.25, 
               label=load_type)
    
    ax.set_xlabel('Server Type')
    ax.set_ylabel('Requests/Second')
    ax.set_title('Throughput Comparison')
    ax.set_xticks(x + 0.25)
    ax.set_xticklabels(servers)
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    # 2. Mean latency
    ax = axes[0, 1]
    for load_type in load_types:
        latency_values = []
        for server in servers:
            if load_type in results[server]:
                latency_values.append(results[server][load_type]['mean_time'])
            else:
                latency_values.append(0)
        
        x = np.arange(len(servers))
        ax.bar(x + load_types.index(load_type) * 0.25, latency_values, 0.25, 
               label=load_type)
    
    ax.set_xlabel('Server Type')
    ax.set_ylabel('Mean Latency (ms)')
    ax.set_title('Latency Comparison')
    ax.set_xticks(x + 0.25)
    ax.set_xticklabels(servers)
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    # 3. P95 latency
    ax = axes[1, 0]
    for server in servers:
        p95_values = []
        for load_type in load_types:
            if load_type in results[server]:
                p95_values.append(results[server][load_type]['p95_time'])
            else:
                p95_values.append(0)
        
        ax.plot(load_types, p95_values, marker='o', label=server)
    
    ax.set_xlabel('Load Type')
    ax.set_ylabel('P95 Latency (ms)')
    ax.set_title('95th Percentile Latency')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # 4. Error rates
    ax = axes[1, 1]
    for server in servers:
        error_rates = []
        for load_type in load_types:
            if load_type in results[server]:
                total = results[server][load_type]['total_requests']
                errors = results[server][load_type]['failed_requests']
                error_rate = (errors / total * 100) if total > 0 else 0
                error_rates.append(error_rate)
            else:
                error_rates.append(0)
        
        ax.plot(load_types, error_rates, marker='o', label=server)
    
    ax.set_xlabel('Load Type')
    ax.set_ylabel('Error Rate (%)')
    ax.set_title('Error Rate Comparison')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('server_performance_comparison.png', dpi=150)
    print("\n✓ Performance chart saved as 'server_performance_comparison.png'")
    plt.show()

def stress_test_endpoint(base_url, endpoint, duration=30):
    """Stress test a specific endpoint."""
    print(f"\nStress testing {endpoint} for {duration} seconds...")
    
    async def stress_test():
        results = []
        start_time = time.time()
        
        async with aiohttp.ClientSession() as session:
            while time.time() - start_time < duration:
                try:
                    req_start = time.time()
                    async with session.get(base_url + endpoint) as resp:
                        await resp.read()
                        elapsed = time.time() - req_start
                        results.append(elapsed)
                except:
                    pass
        
        return results
    
    # Run with increasing concurrency
    concurrency_levels = [1, 10, 50, 100, 200]
    
    for level in concurrency_levels:
        print(f"\nConcurrency level: {level}")
        
        tasks = []
        for _ in range(level):
            task = asyncio.create_task(stress_test())
            tasks.append(task)
        
        all_results = asyncio.run(asyncio.gather(*tasks))
        
        # Combine results
        combined = []
        for result in all_results:
            combined.extend(result)
        
        if combined:
            print(f"  Total requests: {len(combined)}")
            print(f"  Requests/sec: {len(combined)/duration:.1f}")
            print(f"  Mean latency: {statistics.mean(combined)*1000:.1f} ms")
            print(f"  P95 latency: {np.percentile(combined, 95)*1000:.1f} ms")

def main():
    """Run load testing and benchmarking."""
    print("Load Testing and Benchmarking Tool")
    print("=" * 60)
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
    else:
        print("\nOptions:")
        print("1. Quick test (single server)")
        print("2. Compare servers")
        print("3. Stress test")
        print("4. Real-world simulation")
        
        choice = input("\nEnter choice (1-4): ").strip()
        command = choice
    
    if command in ['1', 'quick']:
        # Quick test
        tester = AsynchronousLoadTester()
        stats = tester.run_test(num_requests=1000, num_concurrent=50)
        
        if stats:
            print("\nResults:")
            print(f"  Total requests: {stats['total_requests']}")
            print(f"  Successful: {stats['successful_requests']}")
            print(f"  Failed: {stats['failed_requests']}")
            print(f"  Duration: {stats['duration']:.2f} seconds")
            print(f"  Requests/second: {stats['requests_per_second']:.1f}")
            print(f"  Mean latency: {stats['mean_time']:.1f} ms")
            print(f"  P95 latency: {stats['p95_time']:.1f} ms")
            print(f"  P99 latency: {stats['p99_time']:.1f} ms")
    
    elif command in ['2', 'compare']:
        # Compare servers
        results = compare_server_performance()
        if results:
            visualize_results(results)
    
    elif command in ['3', 'stress']:
        # Stress test
        endpoint = input("Enter endpoint to test [/]: ").strip() or '/'
        stress_test_endpoint("http://127.0.0.1:8080", endpoint)
    
    elif command in ['4', 'simulate']:
        # Real-world simulation
        simulator = WorkloadSimulator()
        results = asyncio.run(simulator.simulate_real_world_pattern(duration=60))
        print(f"\nSimulation complete. Total requests: {len(results)}")

if __name__ == "__main__":
    main()
