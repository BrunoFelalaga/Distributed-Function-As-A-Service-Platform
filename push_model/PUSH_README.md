## Architecture Overview

The push model implementation consists of a dispatcher and worker components that communicate via ZMQ DEALER/ROUTER sockets. The dispatcher actively sends tasks to available workers, which process them asynchronously and return results.

The dispatcher maintains worker state through periodic heartbeats, dynamically assigns tasks based on worker capacity, and provides fault tolerance through task retry mechanisms. Workers maintain three separate socket connections for registration, task reception, and result delivery, all sharing the same worker identity.

## Workflow

1. Dispatcher maintains worker registry and tracks availability through heartbeats
2. Dispatcher receives tasks via Redis pub/sub and queues them
3. Dispatcher proactively pushes tasks to available workers based on capacity
4. Workers execute tasks in their process pool and send results back via result socket
5. Dispatcher tracks task assignments and handles worker failures through heartbeat monitoring
6. Failed tasks are automatically retried on different workers up to MAX_RETRIES

## Push Dispatcher Overview

### Fault Tolerance
The system implements robust fault tolerance through worker heartbeat monitoring and task retry mechanisms. Failed workers are detected when heartbeats stop, and their tasks are automatically reassigned to healthy workers. Tasks are given multiple chances (up to MAX_RETRIES) to complete successfully, distinguishing between transient worker failures and inherently problematic tasks. This approach ensures high system reliability without wasting resources on unexecutable tasks.

### Handling Dead Workers
- Worker considered dead when `curr_time - last_hb > HEARTBEAT_TIMEOUT`
- Dead worker handling:
  - Remove from available_workers queue
  - Add to dead_workers set
  - Delete from last_worker_heartbeat tracking
  - Process assigned tasks:
    - If task exceeds MAX_RETRIES (3), mark as FAILED with WorkerFailureError
    - Otherwise, reset to QUEUED and enqueue for retry
  - Clean up task_assignments data

### Results Registration
- Poll socket for incoming results
- For failed tasks:
  - Update retry counter
  - If retry count > MAX_RETRIES, mark as FAILED in Redis
  - Otherwise, requeue for retry
- For completed tasks:
  - Update Redis with COMPLETED status and result
  - Remove from worker's task assignments

### Task Assignment
- Get available worker and its remaining capacity
- Fetch tasks from task_queue up to worker capacity
- Update Redis with RUNNING status and worker assignment
- Track assignments for failure detection

### Locks 
- `task_assignment_lock` : For synchronizing task assignment to workers

### Sockets
1. `registration_socket`: Worker registration ROUTER socket on port 5555
2. `task_socket`: Task assignment ROUTER socket on port 5556
3. `results_socket`: Result listener ROUTER socket on port 5557


### Tracking Data structures:

1. `worker_tasks`: Dictionary tracking tasks assigned to each worker
2. `worker_capacities`: Dictionary tracking maximum capacity of each worker
3. `last_worker_heartbeat`: Dictionary tracking last heartbeat time per worker
4. `dead_workers`: Set tracking workers considered dead
5. `available_workers`: Queue for managing available workers
6. `task_queue`: Queue for tasks waiting to be assigned
7. `task_assignments`: Dictionary tracking task IDs assigned to each worker


## Push Worker Overview

### Key Functions
- Registers with dispatcher as READY with capacity information via `registration_socket`
- Starts `heartbeat_thread` and sends heartbeats with idle capacity at `HEARTBEAT_INTERVAL` regularly
- Runs an asynchronous processing pool to execute tasks
  - Within the pool context:
    - Runs a `task_thread` to continuously check task states and failures in `results_queue`
    - Updates the `running_tasks` list by removing completed tasks
    - Polls task_socket for new tasks from dispatcher and submits them into processing pool
- Runs a separate `results_thread` to send results in `results_queue` to dispatcher

### Communication Pattern
- Uses three separate ZMQ DEALER sockets (registration, tasks, results)
- Maintains consistent worker identity across all sockets
- Implements asynchronous message handling via dedicated threads
- Communicates task status through serialized payloads

### Fault Tolerance
- Catches and serializes exceptions during function execution
- Properly handles task failures without crashing the worker
- Categorizes errors (serialization vs. execution) for clearer reporting
- Continues processing other tasks when one fails

### Concurrency Model
- Leverages process pool for CPU-bound parallel execution
- Maintains separate threads for communication and task management
- Uses queue for thread-safe result handling
- Tracks capacity dynamically based on running task count

### sockets

1. `socket_reg`: DEALER socket for registration and heartbeats (port 5555)
2. `socket_task`: DEALER socket for receiving tasks (port 5556)
3. `socket_result`: DEALER socket for sending results (port 5557)

All use same worker identity for consistent tracking by the dispatcher.

# TODO: break up functions into helper functions for cleaner modular code