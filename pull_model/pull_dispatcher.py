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

# Add the parent directory of pull_model to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from serialize import serialize, deserialize
from custom_exceptions import *

# Configure logging
os.makedirs('./logs', exist_ok=True)
logging.basicConfig(filename='./logs/pull_dispatcher.log',
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)


# Locks
socket_lock = threading.Lock()  # For socket operations
task_lock = threading.Lock()    # For task queue operations 
redis_lock = threading.Lock()   # For Redis operations
running_lock = threading.Lock() # For running_tasks dict

# task running configs
DEADLINE_SECONDS = 2  # Configurable deadline duration for task completion
running_tasks = {} # Dictionary to track running tasks and their start times

# Initialize Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Context for ZMQ sockets
context = zmq.Context()

def log_and_print(message, level=logging.INFO):
    """Logs a message and optionally prints it to the console."""
    logging.log(level, message)
    if level in (logging.DEBUG, logging.WARNING, 
                 logging.ERROR, logging.CRITICAL):
        print(message)

def task_listener():
    """Thread to listen for new tasks from Redis."""
    # Create a pubsub object to subscribe to Redis Tasks channels
    pubsub = redis_client.pubsub()
    pubsub.subscribe("Tasks")
    
    while True:
        # continuously get message from channel and process it
        task_message = pubsub.get_message(timeout=1) 
        if task_message and task_message['type'] == 'message':
            log_and_print(f"\nDispatcher received new task message from Redis\n", logging.DEBUG)

            task_id = task_message['data'].decode('utf-8')
            task_data_serialized = redis_client.get(task_id)
            if not task_data_serialized:  # Check if task data is missing
                log_and_print(f"\nInvalid task data for task {task_id}\n", logging.WARNING)
                continue

            try: # Deserialize the task data into a Python object
                task_data = deserialize(task_data_serialized)
            except Exception as e:
                log_and_print(f"\nError deserializing task data for task {task_id}: {e}\n", logging.ERROR)
                continue

            # Finally add the task ID and data to the task queue
            task_queue.put((task_id, task_data))
            log_and_print(f"Task {task_id} added to queue", logging.DEBUG)


def worker_communication_handler():
    """ZMQ REP Thread to communicate with workers, register them, assign tasks and get their results
        Uses locks for every message sending on socket
    """
    
    try: # create socket and bind to port where workers will be communicating at
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:5556")
    except (zmq.ZMQError, Exception) as e:
        log_and_print(f"\nSocket bind failed for port 5556: {e}\n", logging.ERROR)
        return

    while True:
        try:
            message = socket.recv_string()  # No lock needed for basic recv
            
            # Proce task requests
            if message == "REQUEST_TASK":
                log_and_print("REQUEST_TASK received from worker", logging.DEBUG)
                try:
                    
                    with task_lock:# Fetch task with task lock
                        task_id, task_data = task_queue.get(timeout=1)

                    with redis_lock:# Update Redis with redis lock
                        task_data['status'] = "RUNNING"
                        redis_client.set(task_id, serialize(task_data))

                    with running_lock: # Update running tasks with its lock
                        running_tasks[task_id] = time.time()

                    with socket_lock:# Send response with socket lock
                        socket.send_string(serialize(task_data))
                    log_and_print(f"\nAssigned Task: {task_id} to a Worker\n", logging.DEBUG)

                except queue.Empty: # where the queue is empty, send a message to the worker 
                    with socket_lock:
                        socket.send_string("NO_TASKS_AVAILABLE")
                    log_and_print("\nNo tasks available for worker request\n", logging.INFO)

                except Exception as e: # other exceptions are sent as error to the worker
                    with socket_lock:
                        socket.send_string("ERROR_OCCURED")
                    log_and_print(f"\nTask assignment error occurred: {e}\n", logging.ERROR)

            elif message.startswith("RESULT:"):
                try: # process result message
                    result_data = deserialize(message[len("RESULT:"):])
                    task_id, status = result_data['task_id'], result_data['status']

                    with redis_lock: # update tasks state in redis
                        redis_task_data_serialized = redis_client.get(task_id)
                        if redis_task_data_serialized:
                            redis_task_data = deserialize(redis_task_data_serialized)

                             # Only update if the task is running, any other update would conflict with previous status because task could only have been FAILED or COMPLETED
                            if redis_task_data['status'] == 'RUNNING':
                                result = serialize(serialize(result_data['result']))
                                task_data = {"task_id": task_id,
                                             "status": status,
                                             "result": result }
                                redis_client.set(task_id, serialize(result_data))
                                result_print_message = f"\nWorker returned Task {task_id} with status: {status}\n"
                                log_type = logging.DEBUG
                            else: # If job is not running then we ignore the result
                                result_print_message = f"\nTask {task_id} was already marked as {redis_task_data['status']}. Ignoring result.\n"
                                log_type = logging.WARNING

                    with socket_lock: # let worker know result was received. 
                        socket.send_string("RESULT_RECEIVED")
                    log_and_print(result_print_message, log_type)

                # Log exceptions in processing result and notify worker
                except Exception as e: 
                    with socket_lock:
                        socket.send_string("ERROR")
                    log_and_print(f"Error deserializing task result: {e}", logging.ERROR)

        # Catch Exceptions on ZMQ communication 
        except zmq.ZMQError as e:
            log_and_print(f"ZMQ Communication error: {e}\n", logging.ERROR)
            with socket_lock:
                socket.send_string("ERROR")

        # All other communication and task assignment errors
        except Exception as e:
            log_and_print(f"Exception: {e}\n", logging.ERROR)
            with socket_lock:
                socket.send_string("ERROR")


def check_task_timeouts():
    """Thread to periodically check for tasks that have exceeded the deadline."""
    while True:
        failed_tasks = {}  # Dictionary to store tasks that have timed out

        # Iterate through the running tasks to check for timeouts
        for task_id, start_time in list(running_tasks.items()):
            current_time = time.time()  # Get the current time
            # Identify tasks that have exceeded the deadline and put them in failed tasks dict
            if current_time - start_time > DEADLINE_SECONDS:
                failed_tasks[task_id] = (start_time, current_time)

        # Mark tasks as FAILED if they have timed out
        for task_id in failed_tasks.keys():
            task_data_serialized = redis_client.get(task_id)
            if task_data_serialized:
                try:
                    task_data = deserialize(task_data_serialized)  # Deserialize the task data
                    
                    # Only update tasks that are still marked as "RUNNING", else they should not be changed
                    if task_data['status'] == 'RUNNING':
                        # Calculate exceeded time
                        exceeded_time = failed_tasks[task_id][1] - failed_tasks[task_id][0] - DEADLINE_SECONDS
                        
                        # Create WorkerFailureError with exceeded time so client has the information
                        error = WorkerFailureError(f"Task exceeded deadline of {DEADLINE_SECONDS}s (took {exceeded_time:.2f}s)",
                                                    task_id )
                        error_data = handle_task_failure(task_id, error)
                        
                        # Update task data in redis with status and error object
                        task_data.update({'status': 'FAILED',
                                          'result': serialize(serialize(error_data))})
                        redis_client.set(task_id, serialize(task_data))  # Update task status in Redis

                        # Remove the task from the running list
                        running_tasks.pop(task_id, None)  
                        log_and_print(f"\nTask {task_id} has exceeded the deadline by {exceeded_time:.2f} and is marked as FAILED\n", logging.WARNING)
                
                except Exception as e:
                    log_and_print(f"\nError marking task {task_id} as FAILED: {e}\n", logging.ERROR)

        time.sleep(0.5)  # Wait for 0.5 second before checking again

        
if __name__ == "__main__":
    # :> ./logs/pull_dispatcher.log && python3 pull_model/pull_dispatcher.py
    
    # Log that the Pull Dispatcher has started
    log_and_print(f"Pull Dispatcher started", logging.DEBUG)

    # Initialize a thread-safe queue for tasks
    task_queue = queue.Queue()

    # Start a thread for listening to new tasks from Redis
    threading.Thread(target=task_listener, daemon=True).start()

    # Start a thread for handling communication with workers
    threading.Thread(target=worker_communication_handler, daemon=True).start()

    # Start a thread for monitoring task timeouts
    threading.Thread(target=check_task_timeouts, daemon=True).start()

    # Keep the main thread alive to allow daemon threads to run
    while True:
        time.sleep(1)
