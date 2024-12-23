# push_dispatcher.py
import zmq
import redis
import time
import uuid
import threading
import queue
import dill
import codecs
import logging
import sys
import os

# Add the parent directory of pull_model to sys.path for importing shared utilities
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from serialize import serialize, deserialize #, #serialize_tst, deserialize_tst
from custom_exceptions import  * 

# Configure logging to log messages to a file with specified format and log level
os.makedirs('./logs', exist_ok=True)
logging.basicConfig(
    filename='./logs/push_dispatcher.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # Set log level to DEBUG to capture all messages
)

# Function to log a message and optionally print it
def log_and_print(message, level=logging.INFO):
    """Logs a message and optionally prints it to the console."""
    
    # Log the message at the given level
    logging.log(level, message)

    # Print message to the console for specific log levels
    if level in (logging.DEBUG, logging.WARNING, logging.ERROR, logging.CRITICAL):
        print(message)


# Global variables to track worker and task status
global worker_tasks, worker_capacities, last_worker_heartbeat, dead_workers
worker_tasks = {}  # Track current tasks assigned to each worker
worker_capacities = {}  # Track maximum capacity of each worker
available_workers = queue.Queue()  # Queue for managing available workers
task_queue = queue.Queue()  # Queue for tasks that need to be assigned
last_worker_heartbeat = {}  # Track the last heartbeat time for each worker
dead_workers = set()  # Track workers that are considered dead

# Configurable maximum retries for tasks when workers fail
MAX_RETRIES = 3

# Configurable heartbeat settings for worker health monitoring
HEARTBEAT_INTERVAL = 1.0  # Interval between worker heartbeats in seconds
MISSED_HEARTBEATS_THRESHOLD = 3  # Number of missed heartbeats before considering worker as dead
HEARTBEAT_TIMEOUT = HEARTBEAT_INTERVAL * MISSED_HEARTBEATS_THRESHOLD  # Timeout for worker health

# Lock for synchronizing task assignment to workers
task_assignment_lock = threading.Lock()
task_assignments = {}  # Track the tasks assigned to each worker

# Initialize Redis client to interact with the Redis server
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Initialize ZMQ context for managing ZeroMQ sockets
context = zmq.Context()


def worker_registration_listener():
   """Thread with socket for listening to workers and registering them"""
   try:
       
    socket = context.socket(zmq.ROUTER)
    socket.bind("tcp://*:5555")
   except zmq.ZMQError as e:
        log_and_print(f"\nSocket bind failed for port 5555: {e}\n", logging.ERROR)
        return

   test_hb = 0 # to only print to terminal the first hearbeat from worker. all others are logged

   def check_worker_health():
    """Checking whether workers are alive or not"""
    global worker_tasks, last_worker_heartbeat, dead_workers, task_assignments
    while True: # check worker last heartbeat times and see if they are dead by compring with the current time
        for worker_id, last_hb in list(last_worker_heartbeat.items()):
            curr_time = time.time()
            if curr_time - last_hb > HEARTBEAT_TIMEOUT: 
                # ------------------------------print(f"\n\nkkkooo000{curr_time , last_hb ,(curr_time - last_hb), HEARTBEAT_TIMEOUT}\n\n")
                try: # Remove worker from available before marking dead
                    available_workers.queue.remove(worker_id)  
                except (ValueError, Exception) as e:
                    log_message, log_level = f"Unexpected error while removing worker: {str(e)}", logging.ERROR
                    if isinstance(e, ValueError):
                        log_message, log_level = f"Worker ID not found in queue: {str(e)}", logging.WARNING
                    log_and_print(log_message, log_level)
                
                # add to dead workers and delete from heartbeads and reset tasks number in worker tasks
                dead_workers.add(worker_id)
                del last_worker_heartbeat[worker_id]
                worker_tasks[worker_id] = 0
                log_and_print(f"Worker {worker_id} FAILED. --> Removed worker from available_workers and registered in dead_workers", logging.WARNING)

                tasks_for_worker = task_assignments.get(worker_id, set())
                for task_id in tasks_for_worker: # check all the tasks assigned to the worker and update their states in redis or add to task queue
                    task = redis_client.get(task_id)
                    
                    if task:
                        try:
                            task_data = deserialize(task)
                            
                            # Only requeue if task is still RUNNING and was assigned to this worker
                            if (task_data.get('status') == 'RUNNING' and 
                                task_data.get('worker_id') == worker_id.decode()):

                                task_data['retry_count'] = 1 + (task_data.get('retry_count') or 0)
                                if task_data['retry_count'] > MAX_RETRIES:
                                    # Create worker failure error
                                    error = WorkerFailureError(
                                        f"Worker failed after {MAX_RETRIES} retries",
                                        task_id,
                                        worker_id.decode()
                                    )
                                    error_data = handle_task_failure(task_id, error)
                                    
                                    # Update task with failure info
                                    task_data.update({
                                        'status': 'FAILED',
                                        'error': error_data,
                                        'result': serialize(error_data)
                                    })
                                    # task_data['status'] = 'FAILED'
                                    redis_client.set(task_id, serialize(task_data))
                                else:
                                    task_data['status'] = 'QUEUED'
                                    redis_client.set(task_id, serialize(task_data))
                                    task_queue.put((task_id, task_data))

                                log_and_print(f"Worker {worker_id} failed. Re-queuing running task {task_id}.", logging.ERROR )
                            else:
                                log_and_print( f"Not re-queuing task {task_id} (status: {task_data.get('status')})", logging.DEBUG )

                        except Exception as e:
                            log_and_print(f"Error processing task {task_id}: {e}", logging.ERROR)
                            continue
                
                # Clean up worker's task assignments
                if worker_id in task_assignments:
                    del task_assignments[worker_id]

        time.sleep(5)
   worker_health_thread = threading.Thread(target=check_worker_health, daemon=True)
   worker_health_thread.start()

   while True: # get worker messages and process if its a heartbeat or ready message
       worker_id, _, message = socket.recv_multipart()
       msg_data = deserialize(message.decode())

       if isinstance(msg_data, dict) and 'type' in msg_data:
           if msg_data['type'] == "READY": # 
               worker_capacities[worker_id] = msg_data['capacity']
               worker_tasks[worker_id] = 0
               last_worker_heartbeat[worker_id] = time.time()
               if worker_id not in dead_workers:
                   available_workers.put(worker_id)
                   log_and_print(f"\nWorker {worker_id} registered with capacity {msg_data['capacity']}\n", logging.DEBUG)
           
           elif msg_data['type'] == "HEARTBEAT":
               if worker_id not in dead_workers:
                   last_worker_heartbeat[worker_id] = time.time()
                   worker_capacities[worker_id] = msg_data['capacity']
                   if test_hb == 0:
                       log_and_print(f"\nReceived heartbeat from Worker {worker_id}, capacity: {msg_data['capacity']}\n", logging.DEBUG)
                       test_hb += 1
                   else:
                       log_and_print(f"\nReceived heartbeat from Worker {worker_id}, capacity: {msg_data['capacity']}\n", logging.INFO)
   return





def task_assignment_handler():
    try:
        socket = context.socket(zmq.ROUTER)
        socket.bind("tcp://*:5556")
    except zmq.ZMQError as e:
        log_and_print(f"\nSocket bind failed for port 5556: {e}\n", logging.ERROR)
        return

    while True:
        task_id, task_data = task_queue.get()
        worker_id = available_workers.get()

        with task_assignment_lock:
            available_capacity = worker_capacities[worker_id] - worker_tasks[worker_id]
            tasks_to_assign = []
            
            # Collect up to available_capacity tasks
            tasks_to_assign.append((task_id, task_data))
            while len(tasks_to_assign) < available_capacity and not task_queue.empty():
                try:
                    next_task_id, next_task_data = task_queue.get_nowait()
                    tasks_to_assign.append((next_task_id, next_task_data))
                except queue.Empty:
                    break

            # Assign collected tasks
            for task_id, task_data in tasks_to_assign:
                task_data_serialized = redis_client.get(task_id)
                if task_data_serialized:
                    task_data = deserialize(task_data_serialized)
                    if task_data.get('status') in ["RUNNING", "COMPLETED", "FAILED"]:
                        continue

                try:
                    socket.send_multipart([worker_id, b"", serialize(task_data).encode()])
                    worker_tasks[worker_id] += 1
                    
                    task_data['worker_id'] = worker_id.decode()
                    task_data['status'] = "RUNNING"
                    redis_client.set(task_id, serialize(task_data))

                    if worker_id not in task_assignments:
                        task_assignments[worker_id] = set()
                    task_assignments[worker_id].add(task_id)

                except Exception as e:
                    log_and_print(f"\nError assigning Task {task_id} to Worker {worker_id}: {e}\n", logging.ERROR)

            if worker_tasks[worker_id] < worker_capacities[worker_id]:
                available_workers.put(worker_id)


def task_listener():
    """Thread to listen for new tasks from Redis."""
    
    # Create a Redis PubSub instance and subscribe to the "Tasks" channel
    pubsub = redis_client.pubsub()
    pubsub.subscribe("Tasks")

    while True:
        # Wait for messages from the "Tasks" channel
        task_message = pubsub.get_message(timeout=1)
        if task_message and task_message['type'] == 'message':
            task_id = task_message['data'].decode('utf-8')  # Get the task ID from the message
            task_data_serialized = redis_client.get(task_id)  # Retrieve serialized task data from Redis
            if not task_data_serialized:
                log_and_print(f"\nInvalid task data for task {task_id}\n", logging.WARNING)
                continue

            try:
                # Deserialize the task data to get the original object

                task_data = deserialize(task_data_serialized)
            except Exception as e:
                # Log an error if deserialization fails
                log_and_print(f"\nError deserializing task data for task {task_id}: {e}\n, {type(task_data_serialized)}", logging.ERROR)
                continue

            # Add the task to the queue for processing
            task_queue.put((task_id, task_data))
            log_and_print(f"\nTask {task_id} added to queue\n", logging.DEBUG)


def result_listener():
    """Thread to listen for results from workers."""

    # Bind ZMQ socket for receiving results from workers on port 5557
    try:
        socket = context.socket(zmq.ROUTER)
        socket.bind("tcp://*:5557")
    except zmq.ZMQError as e:
        log_and_print(f"\nSocket bind failed for port 5556: {e}\n", logging.ERROR)
        return

    while True:
        # Poll the socket with a timeout of 1000 ms
        if not socket.poll(1000):
            continue

        # Receive message parts from the socket
        parts = socket.recv_multipart()

        # Expect 4 parts in the message (worker ID, separator, separator, message)
        if len(parts) == 4:
            worker_id, _, _, message = parts

            try: # Deserialize the task result message
                result_data = deserialize(message)
            except Exception as e:
                log_and_print(f"\nError deserializing task result from worker {worker_id}: {e}\n", logging.ERROR)
                continue

            # Extract task ID, status, and result from the message
            task_id = result_data['task_id']
            status = result_data['status']
            result_payload = result_data.get('result', None)

            # Handle failed tasks and retry if allowed
            if status == "FAILED":
                task_data = deserialize(redis_client.get(task_id))
                task_data['retry_count'] = 1 + task_data.get('retry_count', 0) #+ 1

                # Remove the task from worker's assignment list
                with task_assignment_lock:
                    if worker_id in task_assignments and task_id in task_assignments[worker_id]:
                        task_assignments[worker_id].remove(task_id)
                        worker_tasks[worker_id] -= 1

                # If retry limit exceeded, mark as failed; otherwise, requeue the task
                if task_data['retry_count'] > MAX_RETRIES:
                    task_data = {"status": "FAILED", "result": result_payload}
                    redis_client.set(task_id, serialize(task_data))
                else:
                    task_data['status'] = "QUEUED"
                    redis_client.set(task_id, serialize(task_data))
                    task_queue.put((task_id, task_data))
            else:
                # Update task status in Redis for successful tasks
                task_data = {"status": status, "result": result_payload}
                redis_client.set(task_id, serialize(task_data))

            # If task is complete, remove it from worker's assignment list
            if status == 'COMPLETED':
                with task_assignment_lock:
                    if worker_id in task_assignments and task_id in task_assignments[worker_id]:
                        task_assignments[worker_id].remove(task_id)
                        worker_tasks[worker_id] -= 1
                        log_and_print(f"\nTask: {task_id} COMPLETED!\nRemoved task from worker {worker_id} assignments\n", logging.DEBUG)

            # Mark the worker as available for new tasks
            available_workers.put(worker_id)
            log_and_print(f"\nWorker {worker_id} returned Task {task_id} with status: {status}\n", logging.DEBUG)



if __name__ == "__main__":
    # Clear existing log and start the push dispatcher
    # :> ./logs/push_dispatcher.log && python3 push_model/push_dispatcher.py

    # Log that the Push Dispatcher has started
    log_and_print(f"\nPush Dispatcher started...\n", logging.DEBUG)
    
    # Start threads to handle worker registration, task listening, task assignment, and result collection
    threading.Thread(target=worker_registration_listener, daemon=True).start()  # Worker registration
    threading.Thread(target=task_listener, daemon=True).start()  # Listening for tasks from Redis
    threading.Thread(target=task_assignment_handler, daemon=True).start()  # Assign tasks to workers
    threading.Thread(target=result_listener, daemon=True).start()  # Collect results from workers

    # Keep the main thread alive indefinitely to maintain the dispatcher
    while True:
        time.sleep(1)

