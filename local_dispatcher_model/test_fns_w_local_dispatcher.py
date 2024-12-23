# from REST import deserialize, serialize
from local_dispatcher import local_dispatcher
from multiprocessing import Pool
import redis
import codecs
import dill
import sys, os

# Add the parent directory of pull_model to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from serialize import serialize, deserialize 
from custom_exceptions import *

redis_client = redis.Redis(host='localhost', port=6379, db=0) # db=0 for default Redis database instance

# Sample function to test
def add(a, b):
    return a + b

# Serialize the function and parameters
serialized_fn = serialize(add)
serialized_params_1 = serialize((1, 2))  # adding 1 and 2
serialized_params_2 = serialize((3, 4))  # adding 3 and 4


# Example task queue for testing
task_queue = [
    ("task1", serialized_fn, serialized_params_1),
    ("task2", serialized_fn, serialized_params_2)
]


if __name__ == "__main__":

    local_dispatcher(task_queue)



    # # Verify results after dispatch
    for task_id in ["task1", "task2"]:
        serialized_result = redis_client.get(task_id)
        if serialized_result:
            task_result = deserialize(serialized_result)
            
            
            print(f"\ntask1 results: \n\t\t\tStatus: {task_result['status']}, \
                    \n\t\t\tFN Execution result: {deserialize_registration(task_result['result'])}\n")
        
        
        else:
            print(f"No result found for {task_id}")
