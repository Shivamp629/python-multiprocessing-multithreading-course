#!/usr/bin/env python3
"""
FastAPI Background Task Patterns

This script demonstrates different approaches to handling background tasks
in FastAPI applications, comparing their characteristics and use cases.
"""

import asyncio
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Task(BaseModel):
    id: str
    status: TaskStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict] = None
    error: Optional[str] = None


# Global task storage (in production, use Redis or a database)
tasks_db: Dict[str, Task] = {}

# Task queue for pattern 3
task_queue = asyncio.Queue()

# Background workers list
background_workers: List[asyncio.Task] = []


async def simulate_heavy_work(duration: float = 2.0, task_id: str = ""):
    """Simulate a time-consuming operation"""
    logger.info(f"Starting heavy work for task {task_id}")
    await asyncio.sleep(duration)
    logger.info(f"Completed heavy work for task {task_id}")
    return {"processed": True, "duration": duration, "task_id": task_id}


def simulate_sync_heavy_work(duration: float = 2.0, task_id: str = ""):
    """Simulate a synchronous time-consuming operation"""
    logger.info(f"Starting sync heavy work for task {task_id}")
    time.sleep(duration)
    logger.info(f"Completed sync heavy work for task {task_id}")
    return {"processed": True, "duration": duration, "task_id": task_id, "sync": True}


# Background worker for queue pattern
async def queue_worker(worker_id: int):
    """Worker that processes tasks from the queue"""
    logger.info(f"Worker {worker_id} started")
    
    while True:
        try:
            # Wait for a task
            task_data = await task_queue.get()
            
            if task_data is None:  # Shutdown signal
                break
            
            task_id = task_data["task_id"]
            duration = task_data.get("duration", 2.0)
            
            # Update task status
            if task_id in tasks_db:
                tasks_db[task_id].status = TaskStatus.RUNNING
                tasks_db[task_id].started_at = datetime.now()
            
            # Process the task
            try:
                result = await simulate_heavy_work(duration, task_id)
                
                if task_id in tasks_db:
                    tasks_db[task_id].status = TaskStatus.COMPLETED
                    tasks_db[task_id].completed_at = datetime.now()
                    tasks_db[task_id].result = result
                    
                logger.info(f"Worker {worker_id} completed task {task_id}")
                
            except Exception as e:
                logger.error(f"Worker {worker_id} failed on task {task_id}: {e}")
                if task_id in tasks_db:
                    tasks_db[task_id].status = TaskStatus.FAILED
                    tasks_db[task_id].error = str(e)
                    tasks_db[task_id].completed_at = datetime.now()
            
            # Mark task as done
            task_queue.task_done()
            
        except asyncio.CancelledError:
            logger.info(f"Worker {worker_id} cancelled")
            break
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {e}")
    
    logger.info(f"Worker {worker_id} stopped")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage background workers lifecycle"""
    # Start background workers
    num_workers = 3
    for i in range(num_workers):
        worker = asyncio.create_task(queue_worker(i))
        background_workers.append(worker)
    
    logger.info(f"Started {num_workers} background workers")
    
    yield
    
    # Shutdown workers
    logger.info("Shutting down workers...")
    
    # Send shutdown signal to all workers
    for _ in background_workers:
        await task_queue.put(None)
    
    # Wait for all workers to complete
    await asyncio.gather(*background_workers, return_exceptions=True)
    
    logger.info("All workers stopped")


app = FastAPI(lifespan=lifespan)


# Pattern 1: FastAPI BackgroundTasks
@app.post("/pattern1/fastapi-background-task")
async def pattern1_background_task(background_tasks: BackgroundTasks, duration: float = 2.0):
    """
    Pattern 1: Using FastAPI's BackgroundTasks
    - Runs AFTER the response is sent
    - Good for simple fire-and-forget operations
    - Limited control and monitoring
    """
    task_id = str(uuid.uuid4())
    
    # Define the background task
    async def bg_task():
        logger.info(f"Background task {task_id} started (Pattern 1)")
        result = await simulate_heavy_work(duration, task_id)
        logger.info(f"Background task {task_id} completed: {result}")
        # In real app, save result to database
    
    # Add task to background tasks
    background_tasks.add_task(bg_task)
    
    return {
        "task_id": task_id,
        "message": "Task will run after this response is sent",
        "pattern": "FastAPI BackgroundTasks"
    }


# Pattern 2: asyncio.create_task (Fire and Forget)
@app.post("/pattern2/asyncio-create-task")
async def pattern2_create_task(duration: float = 2.0):
    """
    Pattern 2: Using asyncio.create_task
    - Starts immediately, runs concurrently
    - No automatic error handling
    - Task runs independently
    """
    task_id = str(uuid.uuid4())
    
    # Create task entry
    task = Task(
        id=task_id,
        status=TaskStatus.PENDING,
        created_at=datetime.now()
    )
    tasks_db[task_id] = task
    
    async def async_task():
        try:
            tasks_db[task_id].status = TaskStatus.RUNNING
            tasks_db[task_id].started_at = datetime.now()
            
            result = await simulate_heavy_work(duration, task_id)
            
            tasks_db[task_id].status = TaskStatus.COMPLETED
            tasks_db[task_id].completed_at = datetime.now()
            tasks_db[task_id].result = result
            
        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            tasks_db[task_id].status = TaskStatus.FAILED
            tasks_db[task_id].error = str(e)
            tasks_db[task_id].completed_at = datetime.now()
    
    # Create the task - it starts running immediately
    asyncio.create_task(async_task())
    
    return {
        "task_id": task_id,
        "message": "Task started and running concurrently",
        "pattern": "asyncio.create_task"
    }


# Pattern 3: Task Queue with Workers
@app.post("/pattern3/task-queue")
async def pattern3_task_queue(duration: float = 2.0, priority: int = 0):
    """
    Pattern 3: Task Queue with Background Workers
    - Controlled concurrency (fixed number of workers)
    - Tasks wait in queue if workers are busy
    - Better for rate-limited operations
    """
    task_id = str(uuid.uuid4())
    
    # Create task entry
    task = Task(
        id=task_id,
        status=TaskStatus.PENDING,
        created_at=datetime.now()
    )
    tasks_db[task_id] = task
    
    # Add to queue
    await task_queue.put({
        "task_id": task_id,
        "duration": duration,
        "priority": priority
    })
    
    return {
        "task_id": task_id,
        "message": "Task added to queue",
        "pattern": "Task Queue with Workers",
        "queue_size": task_queue.qsize()
    }


# Pattern 4: Scheduled Tasks (Cron-like)
scheduled_tasks: Dict[str, asyncio.Task] = {}

@app.post("/pattern4/scheduled-task")
async def pattern4_scheduled_task(delay_seconds: float = 5.0, duration: float = 2.0):
    """
    Pattern 4: Scheduled Tasks
    - Execute task after a delay
    - Can be cancelled before execution
    """
    task_id = str(uuid.uuid4())
    
    # Create task entry
    task = Task(
        id=task_id,
        status=TaskStatus.PENDING,
        created_at=datetime.now()
    )
    tasks_db[task_id] = task
    
    async def scheduled_task():
        try:
            # Wait for the scheduled time
            await asyncio.sleep(delay_seconds)
            
            if task_id not in tasks_db:
                return  # Task was cancelled
            
            tasks_db[task_id].status = TaskStatus.RUNNING
            tasks_db[task_id].started_at = datetime.now()
            
            result = await simulate_heavy_work(duration, task_id)
            
            tasks_db[task_id].status = TaskStatus.COMPLETED
            tasks_db[task_id].completed_at = datetime.now()
            tasks_db[task_id].result = result
            
        except asyncio.CancelledError:
            logger.info(f"Scheduled task {task_id} was cancelled")
            if task_id in tasks_db:
                tasks_db[task_id].status = TaskStatus.FAILED
                tasks_db[task_id].error = "Cancelled"
        except Exception as e:
            logger.error(f"Scheduled task {task_id} failed: {e}")
            if task_id in tasks_db:
                tasks_db[task_id].status = TaskStatus.FAILED
                tasks_db[task_id].error = str(e)
        finally:
            # Remove from scheduled tasks
            if task_id in scheduled_tasks:
                del scheduled_tasks[task_id]
    
    # Create and store the task
    scheduled_task_obj = asyncio.create_task(scheduled_task())
    scheduled_tasks[task_id] = scheduled_task_obj
    
    return {
        "task_id": task_id,
        "message": f"Task scheduled to run in {delay_seconds} seconds",
        "pattern": "Scheduled Task"
    }


@app.delete("/pattern4/scheduled-task/{task_id}")
async def cancel_scheduled_task(task_id: str):
    """Cancel a scheduled task before it executes"""
    if task_id not in scheduled_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Cancel the task
    scheduled_tasks[task_id].cancel()
    
    # Update status
    if task_id in tasks_db:
        tasks_db[task_id].status = TaskStatus.FAILED
        tasks_db[task_id].error = "Cancelled by user"
    
    return {"message": "Task cancelled", "task_id": task_id}


# Pattern 5: Sync task in thread pool
@app.post("/pattern5/sync-in-thread-pool")
async def pattern5_sync_in_thread_pool(duration: float = 2.0):
    """
    Pattern 5: Running sync code in thread pool
    - For CPU-bound or blocking sync operations
    - Prevents blocking the event loop
    """
    task_id = str(uuid.uuid4())
    
    # Create task entry
    task = Task(
        id=task_id,
        status=TaskStatus.PENDING,
        created_at=datetime.now()
    )
    tasks_db[task_id] = task
    
    async def run_sync_task():
        try:
            tasks_db[task_id].status = TaskStatus.RUNNING
            tasks_db[task_id].started_at = datetime.now()
            
            # Run sync function in thread pool
            import anyio
            result = await anyio.to_thread.run_sync(
                simulate_sync_heavy_work, 
                duration, 
                task_id
            )
            
            tasks_db[task_id].status = TaskStatus.COMPLETED
            tasks_db[task_id].completed_at = datetime.now()
            tasks_db[task_id].result = result
            
        except Exception as e:
            logger.error(f"Sync task {task_id} failed: {e}")
            tasks_db[task_id].status = TaskStatus.FAILED
            tasks_db[task_id].error = str(e)
    
    # Create the task
    asyncio.create_task(run_sync_task())
    
    return {
        "task_id": task_id,
        "message": "Sync task running in thread pool",
        "pattern": "Sync in Thread Pool"
    }


# Task status endpoint
@app.get("/task/{task_id}")
async def get_task_status(task_id: str):
    """Get the status of any task"""
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = tasks_db[task_id]
    
    # Calculate duration if completed
    duration = None
    if task.started_at and task.completed_at:
        duration = (task.completed_at - task.started_at).total_seconds()
    
    return {
        "task": task,
        "duration_seconds": duration
    }


# System status
@app.get("/status")
async def system_status():
    """Get overall system status"""
    # Count tasks by status
    status_counts = {}
    for task in tasks_db.values():
        status_counts[task.status] = status_counts.get(task.status, 0) + 1
    
    return {
        "total_tasks": len(tasks_db),
        "status_counts": status_counts,
        "queue_size": task_queue.qsize(),
        "active_workers": len([w for w in background_workers if not w.done()]),
        "scheduled_tasks": len(scheduled_tasks)
    }


# Comparison endpoint
@app.post("/compare-patterns")
async def compare_patterns(duration: float = 1.0, count: int = 5):
    """Run the same workload using different patterns and compare"""
    if count > 20:
        raise HTTPException(status_code=400, detail="Count too high, max 20")
    
    results = {}
    
    # Pattern 2: asyncio.create_task
    start = time.time()
    pattern2_tasks = []
    for _ in range(count):
        response = await pattern2_create_task(duration)
        pattern2_tasks.append(response["task_id"])
    
    # Wait for all to complete
    while any(tasks_db.get(tid, Task(id="", status=TaskStatus.PENDING, created_at=datetime.now())).status 
             in [TaskStatus.PENDING, TaskStatus.RUNNING] for tid in pattern2_tasks):
        await asyncio.sleep(0.1)
    
    results["pattern2_asyncio_create_task"] = time.time() - start
    
    # Pattern 3: Task Queue (limited concurrency)
    start = time.time()
    pattern3_tasks = []
    for _ in range(count):
        response = await pattern3_task_queue(duration)
        pattern3_tasks.append(response["task_id"])
    
    # Wait for all to complete
    while any(tasks_db.get(tid, Task(id="", status=TaskStatus.PENDING, created_at=datetime.now())).status 
             in [TaskStatus.PENDING, TaskStatus.RUNNING] for tid in pattern3_tasks):
        await asyncio.sleep(0.1)
    
    results["pattern3_task_queue"] = time.time() - start
    
    return {
        "workload": {
            "count": count,
            "duration_per_task": duration
        },
        "execution_times": results,
        "analysis": {
            "fastest": min(results, key=results.get),
            "pattern2_desc": "All tasks run concurrently (unlimited)",
            "pattern3_desc": f"Limited to {len(background_workers)} concurrent workers"
        }
    }


if __name__ == "__main__":
    print("\n" + "="*60)
    print("FastAPI Background Task Patterns")
    print("="*60)
    print("\nStarting server on http://localhost:8001")
    print("\nAvailable patterns:")
    print("  1. FastAPI BackgroundTasks - POST /pattern1/fastapi-background-task")
    print("  2. asyncio.create_task - POST /pattern2/asyncio-create-task")
    print("  3. Task Queue - POST /pattern3/task-queue")
    print("  4. Scheduled Tasks - POST /pattern4/scheduled-task")
    print("  5. Sync in Thread Pool - POST /pattern5/sync-in-thread-pool")
    print("\nOther endpoints:")
    print("  - GET /task/{task_id} - Check task status")
    print("  - GET /status - System status")
    print("  - POST /compare-patterns - Compare patterns performance")
    print("\nPress Ctrl+C to stop")
    print("="*60 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8001)
