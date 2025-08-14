# Glossary of Terms

## A

**Accept Queue**
: Kernel data structure holding pending TCP connections waiting to be accepted by the application.

**Async/Await**
: Python syntax for writing asynchronous code that looks synchronous, introduced in Python 3.5.

**Atomic Operation**
: An operation that completes in a single step relative to other threads, preventing race conditions.

## B

**Backlog**
: Maximum number of pending connections in the accept queue for a listening socket.

**Barrier**
: Synchronization primitive that blocks threads until all participating threads reach the same point.

**Buffer Pool**
: Pre-allocated collection of memory buffers reused to avoid allocation overhead.

## C

**Cache Line**
: Smallest unit of data transfer between CPU cache and main memory (typically 64 bytes).

**Context Switch**
: Process of storing and restoring CPU state when switching between threads or processes.

**Coroutine**
: Function that can suspend execution and resume later, enabling cooperative multitasking.

**Critical Section**
: Code segment that accesses shared resources and must not be executed concurrently.

## D

**Deadlock**
: Situation where threads wait indefinitely for resources held by each other.

**DMA (Direct Memory Access)**
: Hardware feature allowing devices to transfer data to/from memory without CPU involvement.

**Descriptor**
: See File Descriptor.

## E

**Epoll**
: Linux scalable I/O event notification mechanism, more efficient than select/poll for many connections.

**Event Loop**
: Programming construct that waits for and dispatches events or messages in a program.

## F

**File Descriptor (FD)**
: Non-negative integer uniquely identifying an open file or socket within a process.

**Fork**
: System call creating a new process by duplicating the calling process.

## G

**GIL (Global Interpreter Lock)**
: Mutex preventing multiple Python threads from executing bytecode simultaneously.

**Green Thread**
: User-space thread scheduled by runtime rather than OS kernel (e.g., coroutines).

## H

**Hardware Interrupt**
: Signal from hardware device to CPU indicating an event needs attention.

## I

**IPC (Inter-Process Communication)**
: Mechanisms for processes to communicate and synchronize (pipes, queues, shared memory).

**I/O Bound**
: Program whose performance is limited by input/output operations rather than CPU.

## J

**Join**
: Operation waiting for a thread or process to complete execution.

## K

**Kernel Space**
: Protected memory area where kernel executes with full hardware access privileges.

**Kqueue**
: BSD/macOS scalable event notification interface similar to Linux epoll.

## L

**Listen Socket**
: Socket in passive mode waiting for incoming connections.

**Livelock**
: Threads actively executing but making no progress due to repeated state changes.

**Lock (Mutex)**
: Mutual exclusion primitive ensuring only one thread accesses a resource at a time.

## M

**Memory Barrier**
: CPU instruction ensuring memory operations complete in order across cores.

**Mode Switch**
: Transition between user mode and kernel mode during system calls.

**Multiprocessing**
: Using multiple processes to achieve parallelism, each with separate memory space.

## N

**Nagle's Algorithm**
: TCP optimization buffering small packets to reduce network overhead.

**NIC (Network Interface Card)**
: Hardware component connecting computer to network.

**NUMA (Non-Uniform Memory Access)**
: Architecture where memory access time depends on memory location relative to processor.

## O

**OSI Model**
: Seven-layer conceptual model describing network communication functions.

## P

**Page Fault**
: Exception when process accesses memory page not currently in physical memory.

**Polling**
: Repeatedly checking resource status rather than waiting for notification.

**Process**
: Instance of executing program with its own memory space and resources.

## Q

**Queue**
: FIFO data structure used for thread/process communication.

## R

**Race Condition**
: Bug occurring when program behavior depends on relative timing of events.

**Reader-Writer Lock**
: Lock allowing concurrent readers or exclusive writer access.

**Ring Buffer**
: Fixed-size circular buffer efficient for streaming data.

## S

**Scheduler**
: OS component deciding which thread/process runs on which CPU core.

**Semaphore**
: Synchronization primitive controlling access to a resource pool.

**Socket**
: Endpoint for network communication between processes.

**Starvation**
: Thread unable to gain required resources due to other threads monopolizing them.

**System Call**
: Request from user program to kernel for privileged operation.

## T

**Thread**
: Lightweight execution unit within process sharing memory space.

**Thread Pool**
: Collection of pre-created threads reused for executing tasks.

**Thread-Local Storage (TLS)**
: Memory area unique to each thread for storing thread-specific data.

**Thundering Herd**
: Multiple processes/threads waking simultaneously when event occurs, causing contention.

**TLB (Translation Lookaside Buffer)**
: CPU cache for virtual-to-physical address translations.

## U

**User Space**
: Memory area where application code executes with restricted privileges.

## V

**Virtual Memory**
: Memory management providing each process illusion of contiguous address space.

## W

**Work Stealing**
: Scheduling strategy where idle threads take work from busy threads' queues.

## Z

**Zero-Copy**
: Techniques avoiding unnecessary data copying between kernel and user space (e.g., sendfile, splice).
