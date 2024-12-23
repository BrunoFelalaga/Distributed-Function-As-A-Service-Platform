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
worker_number = sys.argv[1]


os.makedirs('./logs', exist_ok=True)
with open(f'./logs/pull_worker_{worker_number}.log', 'w') as f:
    f.write("Test log entry\n")

logging.basicConfig(filename=f'./logs/pull_worker_{worker_number}.log',
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

logging.info(f"Worker starting initialization")
logging.info(f"Number of processes: ")

# Lock to ensure synchronized access to the socket
lock = threading.Lock()  



def log_and_print(message, level=logging.INFO):
    """Logs a message and optionally prints it to the console."""
    logging.log(level, message)
    if level in (logging.DEBUG, logging.WARNING, logging.ERROR, logging.CRITICAL):
        print(message)

def execute_fn(task_id: uuid.UUID, ser_fn: str, ser_params: str):
    """Executes the task and returns the serialized result and status."""
    status = "FAILED" # set task as failed to begin with

    # execute task and catch all exceptions in doing that
    try: 
        try: # deserialize function and its parameters
            fn = deserialize(ser_fn)
            params = deserialize(ser_params)
        except Exception as e: # catch exceptions as serialization/deserialization errors
            raise SerializationError(str(e), task_id, operation="deserialization")

        try: # Execute the function with appropriate parameters, parse the parameter datatypes and execute accordingly
            if isinstance(params, tuple):
                if len(params) == 2 and isinstance(params[1], dict):
                    args, kwargs = params
                    result = fn(*args, **kwargs)
                else:
                    result = fn(*params)
            elif isinstance(params, dict):
                result = fn(**params)
            elif isinstance(params, (list, set)):
                result = fn(*params)
            else:
                result = fn(params)

            status = "COMPLETED" # set status to complete if successful execution

        except MemoryError as e: # Memory errors should be caught with separate message if function causes Out Of Memory Errors
            raise FunctionExecutionFailure("Memory allocation failed", task_id, e)
        except Exception as e: # All others will have a string of their errors sent as messages
            raise FunctionExecutionFailure(str(e), task_id, e)

    except FaaSBaseException as faas_exception: # Exceptions caught if they are of the custom exceptoin kinds
        result = handle_task_failure(task_id, faas_exception)
        result = serialize(result)
    except Exception as e: # All others are packaged as serialization/deserialization errors. 
        error = SerializationError(str(e), task_id, operation="execution")
        result = handle_task_failure(task_id, error)
        result = serialize(result)

    # return result payload as dict
    result_payload = {
        "task_id": str(task_id),
        "status": status,
        "result": serialize(result) if not isinstance(result, dict) else result
    }
    return status, serialize(result_payload)


def pull_worker(dispatcher_url, num_processes):
    """ Main function to process all communication, task processing and reporting
    """
    # Create a ZMQ context and a REQ socket
    try: # create socket and bind to port where workers will be communicating at
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.identity = f"worker-{uuid.uuid4()}".encode() # Assign a unique identity to the worker
        socket.connect(dispatcher_url) # Connect to the dispatcher URL

    except (zmq.ZMQError, Exception) as e:
        log_and_print(f"\nSocket connection failed for port 5556: {e}\n", logging.ERROR)
        return

    # Create a process pool with the specified number of processes provided in terminal
    with Pool(processes=num_processes) as pool:
        log_and_print(f"\nA pull worker started with {num_processes} processes\n", logging.DEBUG)
        running_tasks = {}  # Dictionary to track running tasks
        
        while True: # Request new tasks if there is available capacity
            if len(running_tasks) < num_processes: # check capacity
                try: 
                    with lock: # Request a task from the dispatcher with lock
                        socket.send_string("REQUEST_TASK")
                        message = socket.recv()
                    
                    # If message is tasks are available then give them to processor pool to execute
                    if message != b"NO_TASKS_AVAILABLE":  
                        task = deserialize(message)  # Deserialize the task data
                        task_id = task['task_id']
                        
                        # Submit the task to the process pool
                        running_tasks[task_id] = pool.apply_async(  execute_fn, 
                                                                    (task_id, task['fn_payload'], task['param_payload']))
                        log_and_print(f"Started task {task_id}")

                except Exception as e: # Log any errors while requesting a task
                    log_and_print(f"Error requesting task: {e}")
                    time.sleep(1)

            # Check for completed tasks
            completed_tasks = []  # List to keep track of completed tasks
            for task_id, task in running_tasks.items():
                if task.ready():  # Check if the task is completed

                    try: # Get the result from the task
                        status, result_payload_serial = task.get(timeout=0)  # Non-blocking
                        with lock: # Send the result back to the dispatcher with lock
                            result_message = f"RESULT:{result_payload_serial}"
                            socket.send_string(result_message)
                            ack = socket.recv_string()
                            # ack = socket.recv()
                            
                        if ack == "RESULT_RECEIVED":  # If the result was acknowledged add task to completed tasks
                            completed_tasks.append(task_id)
                            log_and_print(f"Completed task {task_id} with status {status}")
                            
                    except Exception as e: # Log errors while handling task results and remove from completed tasks
                        log_and_print(f"Error handling result for task {task_id}: {e}\n", logging.ERROR)
                        completed_tasks.append(task_id) 

            # Remove tasks that have been completed from running tasks
            for task_id in completed_tasks:
                running_tasks.pop(task_id)

            # Sleep briefly to avoid a tight loop
            time.sleep(0.1)


if __name__ == "__main__":
   # Ensure the correct number of arguments are provided  
    if len(sys.argv) != 3:
        print("Usage: python3 pull_worker.py <num_processes> <dispatcher_url>")
        # python3 pull_model/pull_worker.py 1 "tcp://127.0.0.1:5556"
        sys.exit(1)

    # Parse the command-line arguments 
    print(sys.argv[1])  # Print the number of processes (debugging or logging purpose)
    num_processes = int(sys.argv[1])  # Number of worker processes
    dispatcher_url = sys.argv[2]  # Dispatcher URL to connect to
    
   
    # Start the pull worker with the given dispatcher URL and number of processes
    pull_worker(dispatcher_url, num_processes)
