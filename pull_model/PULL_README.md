# Pull Model Dispatcher and Worker README

## Architecture Overview

The pull model implementation consists of a dispatcher and worker components that communicate via ZMQ REQ/REP sockets. Workers request tasks from the dispatcher, execute them, and return results.

## Core Components

### Pull Dispatcher (`pull_dispatcher.py`)

#### Locks
- `socket_lock`: Prevents concurrent access to ZMQ socket, maintaining request/reply message integrity. Dispatcher and Workers grab lock so there is one user when resource is needed
- `task_lock`: Guards the task queue against concurrent modification during task assignment. 
- `redis_lock`: Ensures atomic updates to Redis database entries
- `running_lock`: Protects the running tasks dictionary from concurrent modifications
NOTE: worker_communication_handler fn uses these(but socket_lock) exclusively, hence has priority of resources


#### Threads
1. **Task Listener**: Subscribes to Redis "Tasks" channel to receive new tasks
2. **Worker Communication Handler**: Processes worker requests and results via ZMQ
3. **Task Timeout Checker**: Monitors task execution to enforce deadlines

#### Configuration
- `DEADLINE_SECONDS`: Maximum time allowed for task execution (default: 2s)

### Pull Worker (`pull_worker.py`)

#### Lock
- `socket_lock`: Ensures thread-safe socket operations for task requests/results

#### Functions
- `execute_fn(task_id, ser_fn, ser_params)`: Deserializes and executes functions
- `pull_worker(dispatcher_url, num_processes)`: Main worker function that manages the process pool

## Workflow

1. Dispatcher listens for tasks via Redis pub/sub
2. Workers connect to dispatcher and request tasks when capacity is available
3. Dispatcher assigns tasks and tracks them with execution timestamps
4. Workers execute tasks in their process pool and return results
5. Tasks exceeding the deadline are marked as failed with appropriate error messages

## Error Handling

- Serialization/deserialization errors are captured and reported
- Function execution failures are returned to the client
- Worker timeouts are detected and reported as `WorkerFailureError`
- ZMQ communication errors are handled gracefully

## Starting the System

```bash
# Start the dispatcher
python3 pull_dispatcher.py

# Start a worker with N processes connected to dispatcher URL
python3 pull_worker.py <num_processes> tcp://localhost:5556
```