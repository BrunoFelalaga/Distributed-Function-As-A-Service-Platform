import sys
import os
import time
import redis
from multiprocessing import Pool
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from serialize import serialize, deserialize
from custom_exceptions import *

redis_client = redis.Redis(host='localhost', port=6379, db=0)

def execute_task(task_id, fn_payload, param_payload):
    print(f"EXECUTING TASK: {task_id}")
    try:
        function = deserialize(fn_payload)
        params = deserialize(param_payload)
        
        if isinstance(params, tuple):
            if len(params) == 2 and isinstance(params[1], dict):
                args, kwargs = params
                result = function(*args, **kwargs)
            else:
                result = function(*params)
        elif isinstance(params, (list, set)):
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

def local_dispatcher():
    pubsub = redis_client.pubsub()
    pubsub.subscribe("Tasks")
    print("LOCAL DISPATCHER STARTED")
    
    tasks_in_progress = set()
    
    with Pool(processes=4) as pool:
        while True:
            message = pubsub.get_message()
            if message and message['type'] == 'message':
                task_id = message['data'].decode('utf-8')
                print(f"NEW TASK RECEIVED: {task_id}")
                
                if task_id not in tasks_in_progress:
                    task_data = deserialize(redis_client.get(task_id))
                    task_data['status'] = "RUNNING"
                    redis_client.set(task_id, serialize(task_data))
                    print(f"STATUS: RUNNING for task {task_id}")
                    
                    future = pool.apply_async(
                        execute_task,
                        (task_id, task_data['fn_payload'], task_data['param_payload'])
                    )
                    tasks_in_progress.add((task_id, future))
            
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
                    except Exception as e:
                        print(f"ERROR HANDLING RESULT - Task: {task_id}, Error: {str(e)}")
            
            time.sleep(0.1)

if __name__ == "__main__":
    local_dispatcher()
