# Function As A Service (FaaS) Platform

A distributed system for executing Python functions in a serverless environment, optimized for performance with multiple execution models.


## Architecture Overview

This FaaS platform consists of three main components:
1. **REST API Service**: FastAPI-based service for registering and executing functions
2. **Redis**: Used as both a key-value store and message broker
3. **Task Dispatchers and Workers**: Distribute and execute tasks using various models

### Redis Usage

Redis serves dual purposes:

1. **Key-Value Store**:
   - Storing registered functions and their metadata
   - Maintaining task state throughout execution lifecycle
   - Persisting execution results

2. **Message Broker**:
   - Notifying dispatchers of new tasks via publish/subscribe
   - Decoupling task creation from execution

## Setup

### Prerequisites

- Python 3.x
- Redis server
- Required Python packages

### Installation

```bash
# Install dependencies
pip install -r requirements.txt -q -U
```

## Execution Models

The platform supports three execution models:

1. **Local Dispatcher**: 
   - Baseline implementation using Python's MultiProcessingPool
   - Tasks executed directly in the dispatcher process
   - No network communication overhead
   - Simple but limited to a single machine
   - Useful for development and testing

2. **Push Model (DEALER/ROUTER)**:
   - Dispatcher actively pushes tasks to available workers
   - Uses ZMQ's DEALER/ROUTER pattern for asynchronous communication
   - Worker registration with heartbeat mechanism
   - Better load balancing with active task distribution
   - Fault detection through heartbeats
   - Higher throughput for CPU-intensive workloads

3. **Pull Model (REQ/REP)**:
   - Workers request tasks when they're free
   - Uses ZMQ's REQ/REP pattern for synchronous request-response
   - Self-regulating flow control (workers only request when ready)
   - Simpler implementation with fewer moving parts
   - Better for heterogeneous worker clusters
   - Deadline-based fault detection

## Running the System

### 1. Start the Redis Server

```bash
redis-server
```

### 2. Start the FastAPI Service

```bash
uvicorn REST:app --reload
```

### 3. Start a Task Dispatcher

```bash
python3 task_dispatcher.py -m [local|push|pull] -p <port> -w <num_worker_processors>
```

### 4. Start Workers (for push/pull models)

```bash
# For pull model
python3 pull_model/pull_worker.py <num_processes> <dispatcher_url>

# For push model
python3 push_model/push_worker.py <num_processes> <dispatcher_url>

# Example
python3 push_model/push_worker.py 4 tcp://127.0.0.1:5556
```

## Testing

### Web Service Testing

The platform includes test suites for each execution model.

#### Push Model Testing

```bash
# 1. Clear Redis data
redis-cli flushall

# 2. Start push dispatcher
python3 task_dispatcher.py -m push

# 3. Start push worker(s)
python3 push_model/push_worker.py 4 tcp://127.0.0.1:5556

# 4. Run tests
pytest -s test_webservice_push.py
```

#### Pull Model Testing

```bash
# 1. Clear Redis data
redis-cli flushall

# 2. Start pull dispatcher
python3 task_dispatcher.py -m pull

# 3. Start pull worker(s)
python3 pull_model/pull_worker.py 4 tcp://127.0.0.1:5556

# 4. Run tests
pytest -s test_webservice_pull.py
```

#### Local Model Testing

```bash
# 1. Start local dispatcher
python3 task_dispatcher.py -m local

# 2. Run tests
pytest -s test_webservice_local.py
```

### Performance Testing

Performance tests evaluate throughput, latency, and scalability of each model.

```bash
# Local model testing
python3 task_dispatcher.py -m local &
python3 test_local_dispatcher_performance.py

# Pull model testing
python3 task_dispatcher.py -m pull &
python3 test_push_pull_performance.py -m pull

# Push model testing
python3 task_dispatcher.py -m push &
python3 test_push_pull_performance.py -m push
```

All performance test results are saved to `./testing_plots/comparative`.

## Development Notes

- `my_faas_client.py`: Client library for interacting with the service
- `custom_exceptions.py`: Exception classes for error handling
- `serialize.py`: Serialization/deserialization utilities
- `test_functions.py`: Test functions used in the test suite

## Operations Guide

### Session Management with tmux

#### Redis Session
```bash
# Create session
tmux new-session -s redis

# Run Redis
redis-server &

# Run tests
python3 test_webservice_[push/pull/local].py

# Kill session
tmux kill-session -t redis

# Clear Redis data
redis-cli flushall
```

#### FastAPI Session
```bash
# Create session
tmux new-session -s fastapi

# Start server
uvicorn REST:app --reload

# Kill session
tmux kill-session -t fastapi
```

#### Dispatcher Session
```bash
# Create session
tmux new-session -s dispatcher

# Run dispatcher
python3 task_dispatcher.py -m [local/push/pull] -p <port> -w <num_workers>

# Kill session
tmux kill-session -t dispatcher
```

### tmux Common Commands

| Command | Description |
|---------|-------------|
| `tmux ls` | List sessions |
| `tmux new-session -s <name>` | Create new session |
| `Ctrl+B, D` | Detach from session |
| `tmux attach-session -t <name>` | Reattach to session |
| `tmux kill-session -t <name>` | Kill session |
| `Ctrl+B, [` | Enter scroll mode (use arrow keys to scroll) |
| `q` | Exit scroll mode |

### Debugging Commands

| Command | Description |
|---------|-------------|
| `ps aux \| grep <process>` | Find processes by name |
| `lsof -i :<port>` | Find processes using a port |
| `kill -9 <PID>` | Force kill a process |
| `pkill -f <filename>` | Kill processes by script name |





