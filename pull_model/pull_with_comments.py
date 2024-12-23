import zmq
import redis
import time
import threading
import dill
import codecs
import logging
import sys
import os
import queue

# Add the parent directory of pull_model to sys.path for importing required modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from serialize import serialize, deserialize
from custom_exceptions import *

# Configure logging to track events in a log file
os.makedirs('./logs', exist_ok=True)
logging.basicConfig(filename='./logs/pull_dispatcher.log',
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

# Threading locks for thread-safe operations
socket_lock = threading.Lock()  # Protects socket operations
task_lock = threading.Lock()    # Protects access to the task queue
redis_lock = threading.Lock()   # Protects Redis operations
running_lock = threading.Lock() # Protects the running_tasks dictionary

# Configuration constants
DEADLINE_SECONDS = 2  # Maximum allowed time for task completion
running_tasks = {}     # Tracks running tasks with their start times

# Initialize Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Context for ZMQ communication
context = zmq.Context()

def log_and_print(message, level=logging.INFO):
    """
    Logs a message and optionally prints it to the console.
    Used for unified logging and debugging.
    """
    logging.log(level, message)
    if level in (logging.DEBUG, logging.WARNING, logging.ERROR, logging.CRITICAL):
        print(message)

def task_listener():
    """
    Listens for new tasks published to the Redis "Tasks" channel.
    Adds valid tasks to the task queue for processing.
    """
    # Subscribe to the Redis "Tasks" channel
    pubsub = redis_client.pubsub()
    pubsub.subscribe("Tasks")
    
    while True:
        # Listen for new task messages
        task_message = pubsub.get_message(timeout=1)
        if task_message and task_message['type'] == 'message':
            log_and_print(f"New task message received from Redis.", logging.DEBUG)
            
            task_id = task_message['data'].decode('utf-8')
            task_data_serialized = redis_client.get(task_id)
            if not task_data_serialized:
                # Handle case where task data is missing
                log_and_print(f"Invalid task data for task {task_id}.", logging.WARNING)
                continue

            try:
                # Deserialize the task data
                task_data = deserialize(task_data_serialized)
            except Exception as e:
                # Log deserialization errors
                log_and_print(f"Error deserializing task data for task {task_id}: {e}", logging.ERROR)
                continue

            # Add the task to the task queue
            task_queue.put((task_id, task_data))
            log_and_print(f"Task {task_id} added to the task queue.", logging.DEBUG)

def worker_communication_handler():
    """
    Handles communication between the dispatcher and workers.
    Assigns tasks to workers and processes task results.
    """
    try:
        # Create and bind a ZMQ REP socket for worker communication
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:5556")
    except zmq.ZMQError as e:
        log_and_print(f"Failed to bind socket on port 5556: {e}", logging.ERROR)
        return

    while True:
        try:
            # Receive a message from a worker
            message = socket.recv_string()
            
            if message == "REQUEST_TASK":
                # Handle task request from a worker
                log_and_print("Worker requested a task.", logging.DEBUG)
                try:
                    with task_lock:
                        # Fetch a task from the queue
                        task_id, task_data = task_queue.get(timeout=1)
                    
                    with redis_lock:
                        # Update the task status in Redis
                        task_data['status'] = "RUNNING"
                        redis_client.set(task_id, serialize(task_data))
                    
                    with running_lock:
                        # Add the task to the running tasks list
                        running_tasks[task_id] = time.time()

                    with socket_lock:
                        # Send the task to the worker
                        socket.send_string(serialize(task_data))
                    log_and_print(f"Task {task_id} assigned to worker.", logging.DEBUG)
                except queue.Empty:
                    # Handle case where no tasks are available
                    with socket_lock:
                        socket.send_string("NO_TASKS_AVAILABLE")
                    log_and_print("No tasks available for worker.", logging.INFO)
                except Exception as e:
                    # Handle other errors during task assignment
                    with socket_lock:
                        socket.send_string("ERROR_OCCURRED")
                    log_and_print(f"Task assignment error: {e}", logging.ERROR)

            elif message.startswith("RESULT:"):
                try:
                    # Handle task result from a worker
                    result_data = deserialize(message[len("RESULT:"):])
                    task_id, status = result_data['task_id'], result_data['status']

                    with redis_lock:
                        redis_task_data_serialized = redis_client.get(task_id)
                        if redis_task_data_serialized:
                            redis_task_data = deserialize(redis_task_data_serialized)

                            if redis_task_data['status'] == 'RUNNING':
                                # Update task result in Redis
                                task_data = {
                                    "task_id": task_id,
                                    "status": status,
                                    "result": serialize(result_data['result'])
                                }
                                redis_client.set(task_id, serialize(task_data))
                                log_and_print(f"Task {task_id} completed with status {status}.", logging.DEBUG)
                            else:
                                # Handle cases where task status is not RUNNING
                                log_and_print(f"Ignoring result for task {task_id} (status: {redis_task_data['status']}).", logging.WARNING)

                    with socket_lock:
                        # Acknowledge result receipt to the worker
                        socket.send_string("RESULT_RECEIVED")
                except Exception as e:
                    # Log errors during result processing
                    with socket_lock:
                        socket.send_string("ERROR")
                    log_and_print(f"Error processing task result: {e}", logging.ERROR)
        except zmq.ZMQError as e:
            # Handle ZMQ communication errors
            log_and_print(f"ZMQ communication error: {e}", logging.ERROR)
            with socket_lock:
                socket.send_string("ERROR")
        except Exception as e:
            # Handle unexpected errors
            log_and_print(f"Unexpected error: {e}", logging.ERROR)
            with socket_lock:
                socket.send_string("ERROR")

def check_task_timeouts():
    """
    Periodically checks for tasks that exceed the deadline and marks them as FAILED.
    """
    while True:
        failed_tasks = {}

        # Identify tasks that have timed out
        with running_lock:
            for task_id, start_time in list(running_tasks.items()):
                if time.time() - start_time > DEADLINE_SECONDS:
                    failed_tasks[task_id] = time.time()

        for task_id in failed_tasks:
            task_data_serialized = redis_client.get(task_id)
            if task_data_serialized:
                try:
                    # Deserialize the task data
                    task_data = deserialize(task_data_serialized)
                    if task_data['status'] == 'RUNNING':
                        # Mark the task as FAILED
                        exceeded_time = failed_tasks[task_id] - running_tasks[task_id] - DEADLINE_SECONDS
                        error = WorkerFailureError(f"Task exceeded deadline by {exceeded_time:.2f}s.", task_id)
                        task_data.update({'status': 'FAILED', 'result': serialize(error)})
                        redis_client.set(task_id, serialize(task_data))
                        log_and_print(f"Task {task_id} marked as FAILED due to timeout.", logging.WARNING)

                        with running_lock:
                            running_tasks.pop(task_id, None)
                except Exception as e:
                    log_and_print(f"Error marking task {task_id} as FAILED: {e}", logging.ERROR)

        # Check every 0.5 seconds
        time.sleep(0.5)

if __name__ == "__main__":
    """
    Main entry point for the Pull Dispatcher.
    Starts threads for task listening, worker communication, and task timeout monitoring.
    """
    log_and_print("Pull Dispatcher started.", logging.DEBUG)

    # Initialize a thread-safe task queue
    task_queue = queue.Queue()

    # Start threads for different dispatcher components
    threading.Thread(target=task_listener, daemon=True).start()
    threading.Thread(target=worker_communication_handler, daemon=True).start()
    threading.Thread(target=check_task_timeouts, daemon=True).start()

    # Keep the main thread alive
    while True:
        time.sleep(1)
