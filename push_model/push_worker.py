# push_worker.py
import queue
import zmq
import time
import uuid
import dill
import codecs
import threading
import logging
import sys
import os
from multiprocessing import Pool

# Add the parent directory of pull_model to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from serialize import serialize, deserialize 
from custom_exceptions import *

# Configure logging
logging.basicConfig(
    filename='./logs/push_worker.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # This sets the minimum level to DEBUG to log all messages.
)

HEARTBEAT_INTERVAL = 0.5 #31.0  # seconds between heartbeats

# Function to log and optionally print
def log_and_print(message, level=logging.INFO):
    """Logs a message and optionally prints it to the console."""
    # Log the message at the given level
    logging.log(level, message)
    # Print to console for specific levels including DEBUG
    if level in (logging.DEBUG, logging.WARNING, logging.ERROR, logging.CRITICAL):
        print(message)


def execute_fn(task_id: uuid.UUID, ser_fn: str, ser_params: str):
    """Executes the task and returns the serialized result and status."""
    status = "FAILED"  # Initialize status with "FAILED" by default

    try:
        # Try to deserialize the function and parameters
        try:
            fn = deserialize(ser_fn)
            params = deserialize(ser_params)
        except Exception as e:
            # Handle unexpected exceptions during deserialization
            log_and_print(f"SerializationError{e}", logging.DEBUG)
            raise SerializationError(str(e), task_id, operation="deserialization")

        try:
            # Check the type of parameters and handle accordingly
            if isinstance(params, tuple):
                # If params is a tuple, it might contain (args, kwargs)
                if len(params) == 2 and isinstance(params[1], dict):
                    args, kwargs = params
                    result = fn(*args, **kwargs)  # Call the function with args and kwargs
                else:
                    result = fn(*params)  # Call the function with positional arguments
            elif isinstance(params, dict):
                # If params is a dictionary, treat it as keyword arguments
                result = fn(**params)
            elif isinstance(params, (list, set)):
                # If params is a list or set, treat it as positional arguments
                result = fn(*params)
            else:
                # If params is a single value, pass it directly to the function
                result = fn(params)

            status = "COMPLETED"  # Update status to "COMPLETED" if execution is successful

        except Exception as e:
            # Log and handle any errors that occur during function execution
            
            result = serialize(e)  # Serialize the exception to store it as a result
            status = "FAILED"  # Set status to "FAILED"
            log_and_print(f"\nError executing task: {e}\n", logging.ERROR)
            raise FunctionExecutionFailure(str(e), task_id, e)

    except FaaSBaseException as faas_exception:
        # Convert known FaaS exceptions into error response
        result = handle_task_failure(task_id, faas_exception)
        log_and_print(f"FaaSBaseException{result}", logging.DEBUG)
        result = serialize(result)  # Serialize the dictionary for consistent return format
    except Exception as e:
        # Handle unexpected exceptions during deserialization
        error = SerializationError(str(e), task_id, operation="deserialization")
        result = handle_task_failure(task_id, error)
        result = serialize(result)  # Serialize the dictionary for consistent return format
        log_and_print(f"SerializationError {error}", logging.DEBUG)

    # Serialize the result payload containing task_id, status, and result
    result_payload_serial = serialize({
        "task_id": str(task_id),  # Convert task_id to string
        "status": status,         # Include the task status
        "result": serialize(result) if not isinstance(result, dict) else result  # Serialize the result
    })

    return status, result_payload_serial  # Return the status and serialized result payload


def push_worker(dispatcher_url_reg, dispatcher_url_task, dispatcher_url_result):
   context = zmq.Context()

   # Socket for worker registration and heartbeat (port 5555)
   socket_reg = context.socket(zmq.DEALER)
   socket_reg.identity = f"worker-{uuid.uuid4()}".encode()  # Give a unique identity to each worker
   socket_reg.connect(dispatcher_url_reg)

   # Socket for task assignment (port 5556)
   socket_task = context.socket(zmq.DEALER)
   socket_task.identity = socket_reg.identity  # Use the same identity for consistency
   socket_task.connect(dispatcher_url_task)

   # Socket for sending results (port 5557)
   socket_result = context.socket(zmq.DEALER)
   socket_result.identity = socket_reg.identity  # Use the same identity for consistency
   socket_result.connect(dispatcher_url_result)

   log_and_print(f"\nWorker connected to dispatcher at {dispatcher_url_reg}, \
                 {dispatcher_url_task}, {dispatcher_url_result}\n", logging.DEBUG)

   # Register with the dispatcher as ready
   def send_ready():
       ready_msg = { "type": "READY", "capacity": num_processes }
       socket_reg.send_multipart([b"", serialize(ready_msg).encode()])
       log_and_print(f"\nWorker {socket_reg.identity.decode()} sending READY with capacity {num_processes}\n", logging.DEBUG)

   send_ready()

   # Start the heartbeat thread
   def send_heartbeat():
       test_hb = 0
       while True:
           time.sleep(HEARTBEAT_INTERVAL)  # Send a heartbeat every HEARTBEAT_INTERVAL seconds
           heartbeat_msg = { "type": "HEARTBEAT",
                            "capacity": num_processes - len(running_tasks)}
           socket_reg.send_multipart([b"", serialize(heartbeat_msg).encode()])

           if test_hb == 0:
               log_and_print(f"\nWorker {socket_reg.identity.decode()} sent HEARTBEAT\n", logging.DEBUG)
               test_hb += 1
           else:
               log_and_print(f"\nWorker {socket_reg.identity.decode()} sent HEARTBEAT\n", logging.INFO)

   heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
   heartbeat_thread.start()
   
   # Queue to handle results
   result_queue = queue.Queue()
   running_tasks = {}

   with Pool(processes=num_processes) as pool:
       # Task receiving and processing thread
       def receive_and_process_tasks():
           while True:
               try:
                   # Check completed tasks
                   completed = []
                   for task_id, task in list(running_tasks.items()):
                       if task.ready():
                           try:
                               status, result_payload = task.get()
                               result_queue.put((socket_result.identity, result_payload))
                               completed.append(task_id)
                               log_and_print(f"\nTask {task_id} completed with status {status}\n", logging.DEBUG)
                           except (FunctionExecutionFailure, SerializationError) as e:
                                error_payload = serialize({ "task_id": task_id, "status": "FAILED", 
                                                                "result": serialize(handle_task_failure(task_id, e))})
                                result_queue.put((socket_result.identity, error_payload))
                                completed.append(task_id)
                                log_and_print(f"\nError handling task {task_id}: {e}\n", logging.ERROR)


                   # Remove completed tasks
                   for task_id in completed:
                       running_tasks.pop(task_id)

                   # Only poll for new tasks if we have capacity
                   if len(running_tasks) < num_processes:
                       if socket_task.poll(1000):  # Poll with a timeout of 1 second
                           log_and_print("\nWorker received a task message...\n", logging.DEBUG)
                           message = socket_task.recv_multipart()
                           serialized_task = message[1]
                           task = deserialize(serialized_task)

                           task_id = task['task_id']
                           fn_payload = task['fn_payload']
                           param_payload = task['param_payload']

                           # Submit task to process pool
                           async_result = pool.apply_async(
                               execute_fn, 
                               (task_id, fn_payload, param_payload)
                           )
                           running_tasks[task_id] = async_result
                           log_and_print(f"\nSubmitted Task {task_id} to pool\n", logging.DEBUG)

               except zmq.ZMQError as e:
                   log_and_print(f"\nZMQ Error while receiving tasks: {e}\n", logging.ERROR)
                   time.sleep(0.2)  # Retry if there's an error in receiving/sending

       task_thread = threading.Thread(target=receive_and_process_tasks, daemon=True)
       task_thread.start()

       # Result sending thread
       def send_results():
           while True:
               try:
                   # Get result from the queue and send it
                   identity, result_payload = result_queue.get()
                   socket_result.send_multipart([identity, b"", result_payload.encode()])
                   log_and_print(f"\nWorker {identity.decode()} sent result back to dispatcher\n", logging.DEBUG)
               except Exception as e:
                   log_and_print(f"\nError while sending results: {e}\n", logging.ERROR)
                   time.sleep(0.1)  # Retry if there's an error in sending

       result_thread = threading.Thread(target=send_results, daemon=True)
       result_thread.start()

       # Keep the main thread alive
       while True:
           time.sleep(0.5)
           
if __name__ == "__main__":
   if len(sys.argv) != 3:
       print("Usage: python3 push_worker.py <num_processes> <dispatcher_url>")
       # python3 push_worker.py 1 "tcp://127.0.0.1:5556"
       sys.exit(1)

   num_processes = int(sys.argv[1])  
   dispatcher_url = sys.argv[2]
   base_url = dispatcher_url.replace('"', '').rsplit(':', 1)[0]  
   
   push_worker(
       f"{base_url}:5555",  # Registration port
       f"{base_url}:5556",  # Task port 
       f"{base_url}:5557"   # Result port
   )



