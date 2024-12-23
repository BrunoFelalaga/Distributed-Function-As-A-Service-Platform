from multiprocessing import Pool, cpu_count
import redis
import sys
import os
import logging

# sys.path to import serialize
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from serialize import serialize, deserialize, deserialize_registration

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Set up Redis client with connection pooling
redis_client = redis.Redis(connection_pool=redis.ConnectionPool(host='localhost', port=6379, db=0))

def execute_task(task_id, fn_payload, param_payload):
    """
    Execute a single task.
    """
    logging.info(f"Executing task: {task_id}")

    # Deserialize the function and the parameters
    try:
        function = deserialize_registration(fn_payload)
        param_payload_deserialized = deserialize_registration(param_payload)

        # Execute function with parameters
        if isinstance(param_payload_deserialized, (tuple, list)):
            result = function(*param_payload_deserialized)
        else:
            result = function(param_payload_deserialized)

        status = "COMPLETE"
    except Exception as e:
        result = str(e)
        status = "FAILED"

    # Serialize the result to store in Redis
    return task_id, status, serialize(result)


def local_dispatcher(task_queue):
    """
    Concurrently execute tasks in the task queue using a multiprocessing pool.
    """
    worker_count = cpu_count()
    logging.info(f"Starting dispatcher with {worker_count} worker(s)")

    # Use a multiprocessing pool with dynamic worker processes
    with Pool(processes=worker_count) as pool:
        # Concurrently process tasks
        results = pool.starmap(execute_task, task_queue)

    # Store all results in Redis
    for task_id, status, result_payload in results:
        task_data = {"status": status, "result": result_payload}
        redis_client.set(task_id, serialize(task_data))
        logging.info(f"Task {task_id} stored in Redis with status: {status}")


if __name__ == "__main__":
    # Example serialized functions and parameters
    from serialize import serialize

    def add(a, b):
        return a + b

    serialized_fn = serialize(add)
    task_queue = [
        ("task1", serialized_fn, serialize((2, 3))),  # Task: add(2, 3)
        ("task2", serialized_fn, serialize((10, 5))),  # Task: add(10, 5)
    ]

    # Dispatch tasks
    local_dispatcher(task_queue)
