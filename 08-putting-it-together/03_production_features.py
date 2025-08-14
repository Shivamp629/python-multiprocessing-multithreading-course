#!/usr/bin/env python3
"""
Production-Ready Features

This script demonstrates production features including:
- Connection pooling
- Rate limiting
- Circuit breakers
- Health checks
- Monitoring and metrics
- Graceful shutdown
- Request logging
"""

import asyncio
import time
import threading
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict, deque
from contextlib import asynccontextmanager
import signal
import sys
import psutil
import aiohttp
from aiohttp import web

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RateLimiter:
    """Token bucket rate limiter."""
    
    def __init__(self, rate=100, capacity=100):
        self.rate = rate  # tokens per second
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.time()
        self.lock = asyncio.Lock()
    
    async def acquire(self, tokens=1):
        """Acquire tokens, return True if allowed."""
        async with self.lock:
            now = time.time()
            # Add tokens based on time passed
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

class CircuitBreaker:
    """Circuit breaker for fault tolerance."""
    
    def __init__(self, failure_threshold=5, recovery_timeout=60, success_threshold=2):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.state = 'closed'  # closed, open, half_open
    
    async def call(self, func, *args, **kwargs):
        """Execute function through circuit breaker."""
        if self.state == 'open':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'half_open'
                self.success_count = 0
            else:
                raise Exception("Circuit breaker is open")
        
        try:
            result = await func(*args, **kwargs)
            
            if self.state == 'half_open':
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    self.state = 'closed'
                    self.failure_count = 0
            
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = 'open'
                logger.error(f"Circuit breaker opened after {self.failure_count} failures")
            
            raise e

class ConnectionPool:
    """Async connection pool manager."""
    
    def __init__(self, create_conn_func, max_size=10, max_idle_time=300):
        self.create_conn_func = create_conn_func
        self.max_size = max_size
        self.max_idle_time = max_idle_time
        self.available = asyncio.Queue(maxsize=max_size)
        self.in_use = set()
        self.created = 0
        self.lock = asyncio.Lock()
        self.stats = defaultdict(int)
    
    async def acquire(self):
        """Acquire a connection from the pool."""
        # Try to get from available pool
        try:
            conn, created_at = self.available.get_nowait()
            
            # Check if connection is too old
            if time.time() - created_at > self.max_idle_time:
                await conn.close()
                self.stats['expired'] += 1
            else:
                self.in_use.add(conn)
                self.stats['reused'] += 1
                return conn
        except asyncio.QueueEmpty:
            pass
        
        # Create new connection if under limit
        async with self.lock:
            if self.created < self.max_size:
                conn = await self.create_conn_func()
                self.created += 1
                self.in_use.add(conn)
                self.stats['created'] += 1
                return conn
        
        # Wait for available connection
        conn, created_at = await self.available.get()
        self.in_use.add(conn)
        self.stats['waited'] += 1
        return conn
    
    async def release(self, conn):
        """Release a connection back to the pool."""
        self.in_use.discard(conn)
        try:
            self.available.put_nowait((conn, time.time()))
        except asyncio.QueueFull:
            await conn.close()
            async with self.lock:
                self.created -= 1
    
    async def close_all(self):
        """Close all connections."""
        # Close in-use connections
        for conn in self.in_use:
            await conn.close()
        
        # Close available connections
        while not self.available.empty():
            try:
                conn, _ = self.available.get_nowait()
                await conn.close()
            except asyncio.QueueEmpty:
                break
    
    def get_stats(self):
        """Get pool statistics."""
        return {
            'created': self.stats['created'],
            'reused': self.stats['reused'],
            'expired': self.stats['expired'],
            'waited': self.stats['waited'],
            'in_use': len(self.in_use),
            'available': self.available.qsize(),
            'total': self.created
        }

class MetricsCollector:
    """Collect and expose metrics."""
    
    def __init__(self):
        self.metrics = defaultdict(lambda: defaultdict(float))
        self.histograms = defaultdict(list)
        self.lock = threading.Lock()
    
    def increment(self, metric, value=1, labels=None):
        """Increment a counter metric."""
        with self.lock:
            key = self._make_key(metric, labels)
            self.metrics['counters'][key] += value
    
    def gauge(self, metric, value, labels=None):
        """Set a gauge metric."""
        with self.lock:
            key = self._make_key(metric, labels)
            self.metrics['gauges'][key] = value
    
    def histogram(self, metric, value, labels=None):
        """Record a histogram value."""
        with self.lock:
            key = self._make_key(metric, labels)
            self.histograms[key].append(value)
            
            # Keep only last 1000 values
            if len(self.histograms[key]) > 1000:
                self.histograms[key] = self.histograms[key][-1000:]
    
    def _make_key(self, metric, labels):
        """Create metric key with labels."""
        if labels:
            label_str = ','.join(f"{k}={v}" for k, v in sorted(labels.items()))
            return f"{metric}{{{label_str}}}"
        return metric
    
    def get_metrics(self):
        """Get all metrics in Prometheus format."""
        with self.lock:
            lines = []
            
            # Counters
            for key, value in self.metrics['counters'].items():
                lines.append(f"{key} {value}")
            
            # Gauges
            for key, value in self.metrics['gauges'].items():
                lines.append(f"{key} {value}")
            
            # Histograms (simplified - just show percentiles)
            for key, values in self.histograms.items():
                if values:
                    import numpy as np
                    p50 = np.percentile(values, 50)
                    p95 = np.percentile(values, 95)
                    p99 = np.percentile(values, 99)
                    
                    lines.append(f"{key}_p50 {p50}")
                    lines.append(f"{key}_p95 {p95}")
                    lines.append(f"{key}_p99 {p99}")
            
            return '\n'.join(lines)

class HealthChecker:
    """Health check manager."""
    
    def __init__(self):
        self.checks = {}
        self.status = 'healthy'
        self.last_check = {}
    
    def register_check(self, name, check_func, critical=True):
        """Register a health check."""
        self.checks[name] = {
            'func': check_func,
            'critical': critical,
            'status': 'unknown'
        }
    
    async def run_checks(self):
        """Run all health checks."""
        results = {}
        overall_healthy = True
        
        for name, check in self.checks.items():
            try:
                start = time.time()
                result = await check['func']()
                duration = time.time() - start
                
                results[name] = {
                    'status': 'healthy' if result else 'unhealthy',
                    'duration_ms': duration * 1000,
                    'timestamp': datetime.now().isoformat()
                }
                
                if not result and check['critical']:
                    overall_healthy = False
                    
            except Exception as e:
                results[name] = {
                    'status': 'unhealthy',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
                
                if check['critical']:
                    overall_healthy = False
        
        self.status = 'healthy' if overall_healthy else 'unhealthy'
        self.last_check = results
        
        return overall_healthy, results

class RequestLogger:
    """Structured request logging."""
    
    def __init__(self, app):
        self.app = app
        self.logger = logging.getLogger('access')
    
    @web.middleware
    async def middleware(self, request, handler):
        """Log requests and responses."""
        start_time = time.time()
        
        # Generate request ID
        request_id = f"{time.time()}-{id(request)}"
        request['request_id'] = request_id
        
        try:
            # Process request
            response = await handler(request)
            
            # Log request
            duration = time.time() - start_time
            self.logger.info(json.dumps({
                'request_id': request_id,
                'method': request.method,
                'path': request.path,
                'remote': request.remote,
                'status': response.status,
                'duration_ms': duration * 1000,
                'user_agent': request.headers.get('User-Agent', ''),
                'timestamp': datetime.now().isoformat()
            }))
            
            return response
            
        except Exception as e:
            # Log error
            duration = time.time() - start_time
            self.logger.error(json.dumps({
                'request_id': request_id,
                'method': request.method,
                'path': request.path,
                'remote': request.remote,
                'error': str(e),
                'duration_ms': duration * 1000,
                'timestamp': datetime.now().isoformat()
            }))
            raise

class ProductionServer:
    """Production-ready async HTTP server."""
    
    def __init__(self, host='127.0.0.1', port=8080):
        self.host = host
        self.port = port
        self.app = web.Application()
        self.metrics = MetricsCollector()
        self.health_checker = HealthChecker()
        self.rate_limiters = {}
        self.circuit_breakers = {}
        self.connection_pools = {}
        self.shutdown_event = asyncio.Event()
        
        # Add middleware
        self.app.middlewares.append(self.metrics_middleware)
        self.app.middlewares.append(self.rate_limit_middleware)
        self.app.middlewares.append(RequestLogger(self.app).middleware)
        
        # Setup routes
        self.setup_routes()
        
        # Register health checks
        self.setup_health_checks()
    
    def setup_routes(self):
        """Setup HTTP routes."""
        self.app.router.add_get('/', self.handle_index)
        self.app.router.add_get('/health', self.handle_health)
        self.app.router.add_get('/ready', self.handle_ready)
        self.app.router.add_get('/metrics', self.handle_metrics)
        self.app.router.add_get('/api/data', self.handle_api_data)
        self.app.router.add_post('/api/process', self.handle_api_process)
    
    def setup_health_checks(self):
        """Setup health check functions."""
        # Database check
        async def check_database():
            # Simulate database connectivity check
            return True
        
        # Memory check
        async def check_memory():
            process = psutil.Process()
            memory_percent = process.memory_percent()
            return memory_percent < 80  # Healthy if under 80%
        
        # Disk space check
        async def check_disk():
            disk_usage = psutil.disk_usage('/')
            return disk_usage.percent < 90  # Healthy if under 90%
        
        self.health_checker.register_check('database', check_database, critical=True)
        self.health_checker.register_check('memory', check_memory, critical=False)
        self.health_checker.register_check('disk', check_disk, critical=False)
    
    @web.middleware
    async def metrics_middleware(self, request, handler):
        """Collect metrics for each request."""
        start_time = time.time()
        
        try:
            response = await handler(request)
            duration = time.time() - start_time
            
            # Record metrics
            self.metrics.increment('http_requests_total', 
                                 labels={'method': request.method, 
                                       'endpoint': request.path,
                                       'status': str(response.status)})
            
            self.metrics.histogram('http_request_duration_seconds', 
                                 duration,
                                 labels={'method': request.method,
                                       'endpoint': request.path})
            
            return response
            
        except Exception as e:
            duration = time.time() - start_time
            
            # Record error metrics
            self.metrics.increment('http_requests_total',
                                 labels={'method': request.method,
                                       'endpoint': request.path,
                                       'status': '500'})
            
            self.metrics.increment('http_errors_total',
                                 labels={'method': request.method,
                                       'endpoint': request.path,
                                       'error': type(e).__name__})
            
            raise
    
    @web.middleware
    async def rate_limit_middleware(self, request, handler):
        """Apply rate limiting."""
        # Get or create rate limiter for client
        client_ip = request.remote
        
        if client_ip not in self.rate_limiters:
            self.rate_limiters[client_ip] = RateLimiter(rate=100, capacity=100)
        
        rate_limiter = self.rate_limiters[client_ip]
        
        # Check rate limit
        allowed = await rate_limiter.acquire()
        
        if not allowed:
            self.metrics.increment('rate_limit_exceeded_total',
                                 labels={'client': client_ip})
            
            return web.Response(
                status=429,
                text='Rate limit exceeded',
                headers={'Retry-After': '1'}
            )
        
        return await handler(request)
    
    async def handle_index(self, request):
        """Handle index endpoint."""
        return web.json_response({
            'service': 'Production HTTP Server',
            'version': '1.0.0',
            'timestamp': datetime.now().isoformat()
        })
    
    async def handle_health(self, request):
        """Handle health check endpoint."""
        healthy, results = await self.health_checker.run_checks()
        
        return web.json_response({
            'status': 'healthy' if healthy else 'unhealthy',
            'checks': results,
            'timestamp': datetime.now().isoformat()
        }, status=200 if healthy else 503)
    
    async def handle_ready(self, request):
        """Handle readiness check endpoint."""
        # Check if server is ready to handle requests
        ready = not self.shutdown_event.is_set()
        
        return web.json_response({
            'ready': ready,
            'timestamp': datetime.now().isoformat()
        }, status=200 if ready else 503)
    
    async def handle_metrics(self, request):
        """Handle metrics endpoint."""
        metrics_text = self.metrics.get_metrics()
        
        # Add system metrics
        process = psutil.Process()
        self.metrics.gauge('process_memory_bytes', process.memory_info().rss)
        self.metrics.gauge('process_cpu_percent', process.cpu_percent())
        self.metrics.gauge('process_threads', process.num_threads())
        
        return web.Response(text=metrics_text, content_type='text/plain')
    
    async def handle_api_data(self, request):
        """Handle API data endpoint with circuit breaker."""
        # Get or create circuit breaker for this endpoint
        if 'api_data' not in self.circuit_breakers:
            self.circuit_breakers['api_data'] = CircuitBreaker()
        
        breaker = self.circuit_breakers['api_data']
        
        async def fetch_data():
            # Simulate fetching data from external service
            await asyncio.sleep(0.1)
            
            # Simulate occasional failures
            import random
            if random.random() < 0.1:
                raise Exception("External service error")
            
            return {
                'data': [1, 2, 3, 4, 5],
                'timestamp': datetime.now().isoformat()
            }
        
        try:
            data = await breaker.call(fetch_data)
            return web.json_response(data)
        except Exception as e:
            return web.json_response({
                'error': str(e),
                'circuit_breaker_state': breaker.state
            }, status=503)
    
    async def handle_api_process(self, request):
        """Handle API process endpoint."""
        try:
            data = await request.json()
            
            # Validate input
            if 'input' not in data:
                return web.json_response({
                    'error': 'Missing required field: input'
                }, status=400)
            
            # Process data
            result = {
                'input': data['input'],
                'output': data['input'].upper() if isinstance(data['input'], str) else str(data['input']),
                'processed_at': datetime.now().isoformat()
            }
            
            return web.json_response(result)
            
        except json.JSONDecodeError:
            return web.json_response({
                'error': 'Invalid JSON'
            }, status=400)
    
    async def start(self):
        """Start the production server."""
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        
        logger.info(f"Production server started on {self.host}:{self.port}")
        
        # Setup signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda s, f: asyncio.create_task(self.shutdown()))
        
        # Wait for shutdown
        await self.shutdown_event.wait()
        
        # Graceful shutdown
        logger.info("Starting graceful shutdown...")
        await runner.cleanup()
        
        # Close connection pools
        for pool in self.connection_pools.values():
            await pool.close_all()
        
        logger.info("Server shutdown complete")
    
    async def shutdown(self):
        """Trigger graceful shutdown."""
        logger.info("Shutdown signal received")
        self.shutdown_event.set()

async def main():
    """Run the production server."""
    print("Production-Ready HTTP Server")
    print("=" * 60)
    
    server = ProductionServer()
    
    print(f"\nServer starting on http://{server.host}:{server.port}")
    print("\nEndpoints:")
    print("  /         - Service info")
    print("  /health   - Health check")
    print("  /ready    - Readiness check")
    print("  /metrics  - Prometheus metrics")
    print("  /api/data - API endpoint with circuit breaker")
    print("  /api/process - API endpoint for data processing")
    print("\nFeatures:")
    print("  ✓ Rate limiting (100 req/sec per IP)")
    print("  ✓ Circuit breakers for fault tolerance")
    print("  ✓ Structured logging")
    print("  ✓ Metrics collection")
    print("  ✓ Health checks")
    print("  ✓ Graceful shutdown")
    print("\nPress Ctrl+C to stop\n")
    
    await server.start()

if __name__ == "__main__":
    asyncio.run(main())
