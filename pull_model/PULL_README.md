# Pull Model Architecture Overview

The pull model implementation consists of a dispatcher and worker components that communicate via ZMQ REQ/REP sockets. Workers actively request tasks from the dispatcher, which assigns tasks based on availability and tracks their execution.

The dispatcher listens for tasks via Redis pub/sub, maintains a task queue, and implements a deadline-based failure detection mechanism. Workers connect to the dispatcher, request tasks when they have capacity, and return results upon completion.

## Workflow

1. Dispatcher listens for new tasks from Redis pub/sub channel "Tasks"
2. Workers connect to the dispatcher and request tasks when capacity is available
3. Dispatcher assigns tasks, marks them as "RUNNING" in Redis, and tracks execution times
4. Workers execute tasks in their process pool and send results back to the dispatcher
5. Dispatcher updates Redis with task results and statuses
6. Tasks exceeding the deadline are automatically marked as failed

## Pull Dispatcher Overview

### Fault Tolerance
The system implements robust fault tolerance through a deadline-based task monitoring mechanism. Tasks are tracked from the moment they're assigned, and those exceeding the configurable deadline (default: 2 seconds) are automatically marked as failed with appropriate error information. This approach ensures the system doesn't hang indefinitely waiting for unresponsive workers or problematic tasks.

### Task Lifecycle Management
- Tasks progress through states: QUEUED → RUNNING → COMPLETED/FAILED
- Each state transition is recorded in Redis
- Task execution times are tracked for deadline enforcement

### Thread Architecture
1. **Task Listener Thread**:
   - Subscribes to Redis "Tasks" channel
   - Processes incoming task messages
   - Adds valid tasks to the task queue

2. **Worker Communication Handler Thread**:
   - Handles worker requests via ZMQ REP socket
   - Assigns tasks from queue to requesting workers
   - Processes task results from workers
   - Updates Redis with task statuses and results

3. **Task Timeout Checker Thread**:
   - Periodically scans running tasks
   - Identifies tasks exceeding the deadline
   - Creates WorkerFailureError for timed-out tasks
   - Updates Redis with failure information

### Synchronization Mechanisms
- `socket_lock`: Ensures thread-safe ZMQ socket operations
- `task_lock`: Guards task queue during assignments
- `redis_lock`: Protects Redis operations from race conditions
- `running_lock`: Safeguards running_tasks dictionary updates

### Key Data Structures
- `task_queue`: Thread-safe queue for pending tasks
- `running_tasks`: Dictionary tracking task execution times

## Pull Worker Overview

### Key Functions
- Requests tasks from dispatcher when capacity is available
- Maintains a process pool for concurrent task execution
- Tracks running tasks and reports results
- Handles various error conditions during execution

### Task Execution Process
- `execute_fn` deserializes function and parameters
- Dynamically determines function calling convention based on parameter type
- Categorizes and handles different types of errors
- Returns serialized results with appropriate status

### Communication Pattern
- Uses ZMQ REQ/REP pattern for task requests and result delivery
- Implements thread-safe socket access via locks
- Handles timeouts and communication errors gracefully

### Concurrency Model
- Maintains a configurable pool of worker processes
- Tracks running tasks to prevent exceeding capacity
- Asynchronously collects and reports task results

### Error Handling
- Categorizes errors (serialization, execution, memory)
- Properly serializes exceptions for client reporting
- Maintains worker stability despite task failures

## Implementation Details

### Locks
To ensure thread safety, the dispatcher uses multiple locks:
- `socket_lock`: Prevents concurrent socket operations
- `task_lock`: Protects task queue integrity
- `redis_lock`: Ensures atomic Redis updates
- `running_lock`: Guards the running tasks dictionary

The worker uses a single lock for socket operations to prevent message interleaving.

### Socket Configuration
The dispatcher binds a REP socket to port 5556, while workers connect via REQ sockets. This pattern ensures proper request-response pairing and simplifies message routing.

### Task Monitoring
Tasks are tracked with timestamps from assignment until completion. The timeout checker runs periodically to identify and mark failed tasks, keeping the system responsive even when workers crash or hang.