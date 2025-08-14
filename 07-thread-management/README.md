# Module 7: Thread Management and Synchronization

## Table of Contents

- [Overview](#overview)
- [Learning Objectives](#learning-objectives)
- [Thread Coordination Patterns](#thread-coordination-patterns)
- [Advanced Synchronization Primitives](#advanced-synchronization-primitives)
- [Thread-Local Storage (TLS)](#thread-local-storage-tls)
- [Performance Considerations](#performance-considerations)
- [Common Patterns](#common-patterns)
- [Hands-On Examples](#hands-on-examples)
- [Debugging Multi-threaded Applications](#debugging-multi-threaded-applications)
- [Best Practices](#best-practices)
- [Quick Exercise](#quick-exercise)
- [Key Takeaways](#key-takeaways)
- [Next Module](#next-module)

## Overview

This module provides advanced patterns for thread management and synchronization, building on the basics from Module 3. We'll explore sophisticated coordination techniques, performance optimization, and real-world patterns for building robust multi-threaded applications.

**Duration**: 3 minutes

## Learning Objectives

* Master advanced synchronization patterns
* Understand thread-local storage and context
* Learn thread pool optimization techniques
* Implement producer-consumer and pipeline patterns
* Debug and profile multi-threaded applications

## Thread Coordination Patterns

```mermaid
graph TD
    subgraph "Producer-Consumer"
        P1[Producer 1] --> Q1[Queue]
        P2[Producer 2] --> Q1
        Q1 --> C1[Consumer 1]
        Q1 --> C2[Consumer 2]
    end
    
    subgraph "Pipeline"
        S1[Stage 1] --> Q2[Queue]
        Q2 --> S2[Stage 2]
        S2 --> Q3[Queue]
        Q3 --> S3[Stage 3]
    end
    
    subgraph "Fork-Join"
        M[Master] --> W1[Worker 1]
        M --> W2[Worker 2]
        M --> W3[Worker 3]
        W1 --> J[Join]
        W2 --> J
        W3 --> J
    end
```

## Advanced Synchronization Primitives

### 1. Reader-Writer Locks

Allow multiple readers or single writer.

```mermaid
graph LR
    subgraph "Read Lock"
        R1[Reader 1] --> RESOURCE1[Resource]
        R2[Reader 2] --> RESOURCE1
        R3[Reader 3] --> RESOURCE1
    end
    
    subgraph "Write Lock"
        W[Writer] -->|Exclusive| RESOURCE2[Resource]
    end
    
    style R1 fill:#99ff99
    style R2 fill:#99ff99
    style R3 fill:#99ff99
    style W fill:#ff9999
```

### 2. Barriers

Synchronize threads at specific points.

```mermaid
sequenceDiagram
    participant T1 as Thread 1
    participant T2 as Thread 2
    participant T3 as Thread 3
    participant B as Barrier
    
    T1->>B: Phase 1 complete
    T2->>B: Phase 1 complete
    T3->>B: Phase 1 complete
    B-->>T1: All threads ready
    B-->>T2: All threads ready
    B-->>T3: All threads ready
    
    Note over T1,T3: Phase 2 begins
```

### 3. Semaphore Variants

- **Binary Semaphore**: Like a mutex (0 or 1)
- **Counting Semaphore**: Resource counting
- **Weighted Semaphore**: Variable resource units

## Thread-Local Storage (TLS)

```mermaid
graph TD
    subgraph "Process Memory"
        subgraph "Thread 1"
            TLS1[TLS: conn=DB1]
            STACK1[Stack]
        end
        
        subgraph "Thread 2"
            TLS2[TLS: conn=DB2]
            STACK2[Stack]
        end
        
        subgraph "Shared"
            HEAP[Heap]
            GLOBAL[Globals]
        end
    end
    
    style TLS1 fill:#99ccff
    style TLS2 fill:#99ccff
    style HEAP fill:#ffcc99
```

## Performance Considerations

### Lock Contention Analysis

| Metric | Description | Impact |
|--------|-------------|---------|
| Lock wait time | Time threads spend waiting | Direct performance loss |
| Lock hold time | Duration lock is held | Affects other threads |
| Contention ratio | Waiting time / Total time | Overall efficiency |
| Lock frequency | Acquisitions per second | Overhead indicator |
| Critical section size | Code under lock | Parallelism limiter |

### Thread Affinity

Binding threads to specific CPU cores:

```mermaid
graph LR
    subgraph "Without Affinity"
        TH1[Thread 1] -.-> CPU1[CPU 0]
        TH1 -.-> CPU2[CPU 1]
        TH2[Thread 2] -.-> CPU1
        TH2 -.-> CPU2
    end
    
    subgraph "With Affinity"
        TH3[Thread 1] --> CPU3[CPU 0]
        TH4[Thread 2] --> CPU4[CPU 1]
    end
    
    style TH1 fill:#ffcccc
    style TH3 fill:#ccffcc
```

## Common Patterns

### 1. Work Stealing

Threads steal work from other threads' queues:

```mermaid
graph TD
    subgraph "Thread 1"
        Q1[Queue 1<br/>Tasks: 8]
    end
    
    subgraph "Thread 2"
        Q2[Queue 2<br/>Tasks: 0]
    end
    
    subgraph "Thread 3"
        Q3[Queue 3<br/>Tasks: 2]
    end
    
    Q1 -.->|steal| Q2
    Q3 -.->|steal| Q2
    
    style Q2 fill:#ff9999
```

### 2. Double-Checked Locking

Minimize synchronization overhead:

```python
_instance = None
_lock = threading.Lock()

def get_instance():
    if _instance is None:  # First check (no lock)
        with _lock:
            if _instance is None:  # Second check (with lock)
                _instance = create_instance()
    return _instance
```

### 3. Lock-Free Data Structures

Using atomic operations instead of locks:
- Compare-and-swap (CAS)
- Lock-free queues
- Atomic counters

## Hands-On Examples

### Example 1: Advanced Synchronization (`01_advanced_sync.py`)

```python
# Master advanced synchronization primitives
python 01_advanced_sync.py
```

### Example 2: Thread Coordination Patterns (`02_coordination_patterns.py`)

```python
# Implement coordination patterns
python 02_coordination_patterns.py
```

### Example 3: Performance and Debugging (`03_performance_debug.py`)

```python
# Profile and debug multi-threaded code
python 03_performance_debug.py
```

## Debugging Multi-threaded Applications

### Common Issues

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Deadlock | Threads frozen | Lock ordering, timeouts |
| Livelock | Threads busy but no progress | Backoff strategies |
| Race condition | Intermittent bugs | Proper synchronization |
| Starvation | Some threads never run | Fair scheduling |
| Priority inversion | Low priority blocks high | Priority inheritance |

### Debugging Tools

1. **Thread sanitizers**: Detect races
2. **Lock profilers**: Find contention
3. **Trace tools**: Visualize execution
4. **Core dumps**: Post-mortem analysis

## Best Practices

### 1. Minimize Lock Scope

```python
# Bad: Large critical section
with lock:
    data = fetch_data()      # I/O under lock!
    result = process(data)   # CPU work under lock!
    save_result(result)      # I/O under lock!

# Good: Minimal critical section
data = fetch_data()          # No lock needed
result = process(data)       # No lock needed
with lock:
    shared_results.append(result)  # Only shared access locked
```

### 2. Avoid Nested Locks

```python
# Bad: Nested locks risk deadlock
with lock1:
    with lock2:
        # work...

# Good: Single lock or careful ordering
with combined_lock:
    # work...
```

### 3. Use High-Level Constructs

```python
# Good: Use Queue instead of manual locking
queue = Queue()
queue.put(item)    # Thread-safe
item = queue.get() # Thread-safe
```

## Quick Exercise

Run the examples to explore advanced patterns:

```bash
# 1. Advanced synchronization primitives
python 01_advanced_sync.py

# 2. Coordination patterns
python 02_coordination_patterns.py

# 3. Performance analysis
python 03_performance_debug.py
```

## Key Takeaways

✅ Choose the right synchronization primitive for your use case

✅ Thread-local storage avoids synchronization for per-thread data

✅ Profile to identify lock contention bottlenecks

✅ Use established patterns like producer-consumer

✅ Test thoroughly - concurrency bugs are hard to reproduce

## Next Module

[Module 8: Putting It All Together](../08-putting-it-together/README.md) - Build a high-performance API server
