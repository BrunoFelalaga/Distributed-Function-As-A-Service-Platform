import sys
import os
import time
import redis
from multiprocessing import Pool
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from serialize import serialize, deserialize
from custom_exceptions import *

redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Execute task by unacking arguments from parameter payload into function(from function payload)
def execute_task(task_id, fn_payload, param_payload):
    print(f"EXECUTING TASK: {task_id}")
    try:
        function = deserialize(fn_payload)
        params = deserialize(param_payload)
        
        # Check collection type
        if isinstance(params, tuple): 
            if len(params) == 2 and isinstance(params[1], dict): # arg and kwarg in (_, dict)
                args, kwargs = params
                result = function(*args, **kwargs)
            else: # just args
                result = function(*params)
        elif isinstance(params, (list, set)): # 
            result = function(*params)
        else:
            result = function(params)
            
        status = "COMPLETED"
        print(f"STATUS: {status} for task {task_id}")
        
    except Exception as e:
        status = "FAILED"
        result = str(e)
        print(f"STATUS: {status} for task {task_id} - Error: {str(e)}")

    return task_id, status, serialize(result)

# NOTE: 
# Each Redis PubSub client processes messages independently at their own pace and exactly once by get_message()
# Once retrieved, a message is considered "consumed" by that client
# Multiple clients can all receive the same messages independently
# This is different from a traditional message queue where messages are consumed globally when processed by any consumer.
def local_dispatcher():
    """
    Concurrently execute tasks in the task queue using a multiprocessing pool.
    """

    # Get pubsub client obj and subscribe to Tasks channels
    pubsub = redis_client.pubsub() 
    pubsub.subscribe("Tasks")
    print("LOCAL DISPATCHER STARTED")
    
    tasks_in_progress = set()
    
    with Pool(processes=4) as pool:
        while True:

            # Check PubSub for messages and process tasks in Redis
            message = pubsub.get_message() 
            if message and message['type'] == 'message':
                task_id = message['data'].decode('utf-8')
                print(f"NEW TASK RECEIVED: {task_id}")
                
                # Place tasks in Redis as RUNNING and set worker pool to execute them
                if task_id not in tasks_in_progress:# Only process if not already in progress
                    task_data = deserialize(redis_client.get(task_id))
                    task_data['status'] = "RUNNING"
                    redis_client.set(task_id, serialize(task_data))
                    print(f"STATUS: RUNNING for task {task_id}")
                    
                    future = pool.apply_async(  # worker pool to execute
                        execute_task,
                        (task_id, task_data['fn_payload'], task_data['param_payload'])
                    )
                    tasks_in_progress.add((task_id, future)) # Track task and its future
            
            # Check for tasks(futures) in progress that are ready and  update Redis with the results
            for task_id, future in list(tasks_in_progress):
                if future.ready():
                    try:
                        task_id, status, result = future.get()
                        task_data = {
                            "status": status,
                            "result": result
                        }
                        redis_client.set(task_id, serialize(task_data))
                        print(f"RESULT STORED - Task: {task_id}, Status: {status}")
                        tasks_in_progress.remove((task_id, future))

                    except Exception as e: # Handle Errors
                        print(f"ERROR HANDLING RESULT - Task: {task_id}, Error: {str(e)}")
            
            time.sleep(0.1) # Short sleep to prevent CPU overuse and resume 

if __name__ == "__main__":
    local_dispatcher()
