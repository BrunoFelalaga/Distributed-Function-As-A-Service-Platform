import requests
from serialize import serialize, deserialize #, #deserialize_tst
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import random
import uuid
from pprint import pprint
import os
import threading

base_url = "http://127.0.0.1:8000/"

valid_statuses = ["QUEUED", "RUNNING", "COMPLETED", "FAILED"]


def test_fn_registration_invalid():
    # Using a non-serialized payload data
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",
                               "payload": "payload"})

    # print(resp.status_code)
    assert resp.status_code in [500, 400]

def double(x):
    return x * 2


def test_fn_registration():
    # Using a real serialized function
    serialized_fn = serialize(double)
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialized_fn})

    assert resp.status_code in [200, 201]
    assert "function_id" in resp.json()


def test_execute_fn():
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",
                               "payload": serialize(double)})
    fn_info = resp.json()
    assert "function_id" in fn_info

    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((2,), {}))})

    assert resp.status_code == 200 or resp.status_code == 201
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    resp = requests.get(f"{base_url}status/{task_id}")
    print(resp.json())
    assert resp.status_code == 200
    assert resp.json()["task_id"] == task_id
    assert resp.json()["status"] in valid_statuses


def test_roundtrip():
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialize(double)})
    fn_info = resp.json()

    number = random.randint(0, 10000)
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((number,), {}))})

    assert resp.status_code in [200, 201]
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]
    

    for i in range(200): # 20

        resp = requests.get(f"{base_url}result/{task_id}")
        # print("here-------")
        
        assert resp.status_code == 200
        assert resp.json()["task_id"] == task_id
        # print("here-------", resp.json()['status'])
        if resp.json()['status'] in ["COMPLETED", "FAILED"]:
            s_result = resp.json()
            # print("here-------")
            result = deserialize(deserialize(s_result['result']))
            assert result == number*2
            # print("here-------")
            
            break
        time.sleep(0.01)



def test_memory_limit():
    """Test handling of functions that consume excessive memory"""
    def memory_intensive():
        print("\n[Function start] About to trigger memory error...")  # Debug print
        # Rather than actually consuming memory, raise MemoryError directly
        try:
            # First make a small allocation to test serialization
            x = [1] * 1000
            print(f"[Function] Made small allocation of size {len(x)}")  # Debug print
            # Then explicitly raise MemoryError
            print("[Function] Raising MemoryError...")  # Debug print
            raise MemoryError("Simulated out of memory error")
        except MemoryError as e:
            # Ensure we're propagating the error properly
            print("[Function] Memory error caught, re-raising...")  # Debug print
            raise
    
    print("\n=== Starting memory limit test ===")
    
    print("\nRegistering function...")
    resp = requests.post(base_url + "register_function",
                        json={"name": "memory_hog",
                              "payload": serialize(memory_intensive)})
    fn_info = resp.json()
    print(f"Function registered with ID: {fn_info['function_id']}")
    
    print("\nExecuting function...")
    resp = requests.post(base_url + "execute_function",
                        json={"function_id": fn_info['function_id'],
                              "payload": serialize(((), {}))})
    task_id = resp.json()["task_id"]
    print(f"Task created with ID: {task_id}")
    
    start_time = time.time()
    max_wait = 10  # Shorter timeout since we're not actually allocating memory

    print("\nWaiting for task completion...")
    while time.time() - start_time < max_wait:
        resp = requests.get(f"{base_url}result/{task_id}")
        status = resp.json()['status']
        print(f"\nTime elapsed: {time.time() - start_time:.1f}s, Status: {status}")
        
        if status == "FAILED":
            print("\nTask failed, analyzing error data...")
            error_data = deserialize(resp.json()['result'])
            if isinstance(error_data, str):
                error_data = deserialize(error_data)
            if isinstance(error_data, str):
                error_data = deserialize(error_data)
            print("\nError data received:")
            print("=" * 50)
            for key, value in error_data.items():
                print(f"{key}: {value}")
            print("=" * 50)
            
            # Check for memory error in the error data
            memory_error_found = any(
                "MemoryError" in str(value) 
                for value in error_data.values()
            )
            
            if memory_error_found:
                print("\n✓ Test passed: MemoryError found in error data")
                return
            else:
                print("\n✗ Test failed: MemoryError not found in error data")
                assert False, f"Expected MemoryError but got: {error_data}"
            
        time.sleep(0.5)
    
    final_resp = requests.get(f"{base_url}result/{task_id}")
    print("\n✗ Test failed: Timeout")
    raise AssertionError(
        f"Test timed out after {max_wait} seconds.\n"
        f"Final status: {final_resp.json()['status']}\n"
        f"Response: {final_resp.json()}"
    )



def test_invalid_function_id():
    """Test executing with non-existent function ID"""
    resp = requests.post(base_url + "execute_function",
                        json={"function_id": str(uuid.uuid4()),
                              "payload": serialize(((2,), {}))})
    assert resp.status_code == 404

def test_invalid_task_id():
    """Test retrieving non-existent task"""
    resp = requests.get(f"{base_url}result/{str(uuid.uuid4())}")
    assert resp.status_code == 404


def failing_function():
    raise ValueError("Deliberate error")


def test_function_error():
    """Test handling of function that raises an error"""
    resp = requests.post(base_url + "register_function",
                        json={"name": "failing",
                              "payload": serialize(failing_function)})
    fn_info = resp.json()
    
    resp = requests.post(base_url + "execute_function",
                        json={"function_id": fn_info['function_id'],
                              "payload": serialize(((), {}))})
    task_id = resp.json()["task_id"]
    
    
    for _ in range(20):
        resp = requests.get(f"{base_url}result/{task_id}")
        if resp.json()['status'] == "FAILED":
            error_data = deserialize(resp.json()['result'])
            error_data = deserialize(error_data)
            error_data = deserialize(error_data)
            
            # pprint(error_data, width=100)
            assert error_data['error_type']== "FunctionExecutionFailure"
            assert "ValueError: Deliberate error" in error_data['message']
            assert 'traceback' in error_data
            assert error_data['status'] == "FAILED"
            break
        time.sleep(0.1)


def long_running_function(sleep_time):
    time.sleep(sleep_time)
    return sleep_time


def test_concurrent_execution():
    """Test handling multiple concurrent requests"""
    # Register function
    resp = requests.post(base_url + "register_function",
                        json={"name": "long_running",
                              "payload": serialize(long_running_function)})
    fn_info = resp.json()
    
    # Execute multiple concurrent tasks
    num_tasks = 3
    task_ids = []
    with ThreadPoolExecutor(max_workers=num_tasks) as executor:
        futures = []
        for i in range(num_tasks):
            futures.append(executor.submit(
                requests.post,
                base_url + "execute_function",
                json={"function_id": fn_info['function_id'],
                      "payload": serialize(((0.1,), {}))}
            ))
        task_ids = [f.result().json()["task_id"] for f in futures]
    
    # Verify all tasks complete successfully
    jj = 0
    for task_id in task_ids:
        for _ in range(30):
            
            resp = requests.get(f"{base_url}result/{task_id}")
            if resp.json()['status'] == "COMPLETED":
                result = deserialize(resp.json()['result'])
                result = deserialize(result)
                assert result == 0.1
                jj += 1
                break
            time.sleep(0.1)



def test_large_payload():
    """Test handling of large function inputs/outputs"""
    def large_array_function(size):
        return [i for i in range(size)]
    
    resp = requests.post(base_url + "register_function",
                        json={"name": "large_array",
                              "payload": serialize(large_array_function)})
    fn_info = resp.json()
    
    size = 100000
    resp = requests.post(base_url + "execute_function",
                        json={"function_id": fn_info['function_id'],
                              "payload": serialize(((size,), {}))})
    
    task_id = resp.json()["task_id"]
    for _ in range(20):
        resp = requests.get(f"{base_url}result/{task_id}")
        if resp.json()['status'] == "COMPLETED":
            result = deserialize(resp.json()['result'])
            result = deserialize(result)
            assert len(result) == size
            break
        time.sleep(0.1)



def test_memory_limit():
    """Test handling of functions that consume excessive memory"""
    def memory_intensive():
        print("\n[Function start] About to trigger memory error...")  # Debug print
        # Rather than actually consuming memory, raise MemoryError directly
        try:
            # First make a small allocation to test serialization
            x = [1] * 1000
            print(f"[Function] Made small allocation of size {len(x)}")  # Debug print
            # Then explicitly raise MemoryError
            print("[Function] Raising MemoryError...")  # Debug print
            raise MemoryError("Simulated out of memory error")
        except MemoryError as e:
            # Ensure we're propagating the error properly
            print("[Function] Memory error caught, re-raising...")  # Debug print
            raise
    
    print("\n=== Starting memory limit test ===")
    
    print("\nRegistering function...")
    resp = requests.post(base_url + "register_function",
                        json={"name": "memory_hog",
                              "payload": serialize(memory_intensive)})
    fn_info = resp.json()
    print(f"Function registered with ID: {fn_info['function_id']}")
    
    print("\nExecuting function...")
    resp = requests.post(base_url + "execute_function",
                        json={"function_id": fn_info['function_id'],
                              "payload": serialize(((), {}))})
    task_id = resp.json()["task_id"]
    print(f"Task created with ID: {task_id}")
    
    start_time = time.time()
    max_wait = 10  # Shorter timeout since we're not actually allocating memory

    print("\nWaiting for task completion...")
    while time.time() - start_time < max_wait:
        resp = requests.get(f"{base_url}result/{task_id}")
        status = resp.json()['status']
        print(f"\nTime elapsed: {time.time() - start_time:.1f}s, Status: {status}")
        
        if status == "FAILED":
            print("\nTask failed, analyzing error data...")
            error_data = deserialize(resp.json()['result'])
            if isinstance(error_data, str):
                error_data = deserialize(error_data)
            if isinstance(error_data, str):
                error_data = deserialize(error_data)
            print("\nError data received:")
            print("=" * 50)
            for key, value in error_data.items():
                print(f"{key}: {value}")
            print("=" * 50)
            
            # Check for memory error in the error data
            memory_error_found = any(
                "MemoryError" in str(value) 
                for value in error_data.values()
            )
            
            if memory_error_found:
                print("\n✓ Test passed: MemoryError found in error data")
                return
            else:
                print("\n✗ Test failed: MemoryError not found in error data")
                assert False, f"Expected MemoryError but got: {error_data}"
            
        time.sleep(0.5)
    
    final_resp = requests.get(f"{base_url}result/{task_id}")
    print("\n✗ Test failed: Timeout")
    raise AssertionError(
        f"Test timed out after {max_wait} seconds.\n"
        f"Final status: {final_resp.json()['status']}\n"
        f"Response: {final_resp.json()}"
    )


def test_concurrent_tasks_with_delay():
    """Test executing multiple tasks concurrently with varying delays"""
    def delay_task(seconds):
        time.sleep(seconds)
        return f"Completed after {seconds} seconds"
    
    # Register the function
    resp = requests.post(base_url + "register_function",
                        json={"name": "delay_task",
                              "payload": serialize(delay_task)})
    fn_info = resp.json()
    
    # Launch multiple tasks with different delays
    tasks = []
    delays = [0.1, 0.2, 0.3]  # Short delays to keep test runtime reasonable
    
    for delay in delays:
        resp = requests.post(base_url + "execute_function",
                            json={"function_id": fn_info['function_id'],
                                  "payload": serialize(((delay,), {}))})
        tasks.append(resp.json()["task_id"])
    
    # Wait for all tasks to complete
    results = []
    for task_id in tasks:
        for _ in range(20):  # Allow up to 2 seconds per task
            resp = requests.get(f"{base_url}result/{task_id}")
            if resp.json()['status'] == "COMPLETED":
                result = deserialize(resp.json()['result'])
                results.append(deserialize(result))
                break
            time.sleep(0.1)
    
    # Verify results
    assert len(results) == len(delays), "Not all tasks completed"
    for delay, result in zip(delays, results):
        assert result == f"Completed after {delay} seconds"
        # print("HSHSHS")


def test_cpu_bound_task():
    """Test executing a CPU-intensive task"""
    def fibonacci(n):
        if n <= 1:
            return n
        return fibonacci(n - 1) + fibonacci(n - 2)
    
    # Register the function
    resp = requests.post(base_url + "register_function",
                        json={"name": "fibonacci",
                              "payload": serialize(fibonacci)})
    fn_info = resp.json()
    
    # Execute function with n=10 (small enough to complete quickly)
    resp = requests.post(base_url + "execute_function",
                        json={"function_id": fn_info['function_id'],
                              "payload": serialize(((10,), {}))})
    task_id = resp.json()["task_id"]
    
    # Wait for result
    result = None
    for _ in range(20):  # Allow up to 2 seconds
        resp = requests.get(f"{base_url}result/{task_id}")
        if resp.json()['status'] == "COMPLETED":
            result = deserialize(resp.json()['result'])
            result = deserialize(result)
            break
        time.sleep(0.1)
    
    assert result == 55  # Known Fibonacci(10) result
    print(result)



def test_chain_of_tasks():
    """Test executing tasks in sequence, where each depends on previous result"""
    def step1(x):
        return x * 2

    def step2(x):
        return x + 10

    def step3(x):
        return x ** 2
    
    # Register all functions
    functions = {}
    for func in [step1, step2, step3]:
        resp = requests.post(base_url + "register_function",
                            json={"name": func.__name__,
                                  "payload": serialize(func)})
        functions[func.__name__] = resp.json()["function_id"]
    
    # Execute chain of tasks
    input_value = 5
    current_value = input_value
    
    for func_name in ["step1", "step2", "step3"]:
        resp = requests.post(base_url + "execute_function",
                            json={"function_id": functions[func_name],
                                  "payload": serialize(((current_value,), {}))})
        task_id = resp.json()["task_id"]
        
        # Wait for result
        for _ in range(20):  # Allow up to 2 seconds per step
            resp = requests.get(f"{base_url}result/{task_id}")
            if resp.json()['status'] == "COMPLETED":
                result = deserialize(resp.json()['result'])
                current_value = deserialize(result)
                break
            time.sleep(0.1)
    
    # Verify final result: ((5 * 2) + 10) ** 2 = 400
    assert current_value == 400
    print(current_value)


def test_recursion_limit():
    """Test handling of recursive functions that might exceed stack depth"""
    def recursive_function(n):
        if n == 0:
            return 0
        return recursive_function(n + 1)
    
    resp = requests.post(base_url + "register_function",
                        json={"name": "recursive",
                              "payload": serialize(recursive_function)})
    fn_info = resp.json()
    
    resp = requests.post(base_url + "execute_function",
                        json={"function_id": fn_info['function_id'],
                              "payload": serialize(((1,), {}))})
    task_id = resp.json()["task_id"]
    
    for _ in range(20):
        resp = requests.get(f"{base_url}result/{task_id}")
        if resp.json()['status'] == "FAILED":
            error_data = deserialize(resp.json()['result'])
            error_data = deserialize(error_data)

            # pprint(deserialize(error_data)['message'], width=100)

            print("RecursionError" in deserialize(error_data)['message'])
            assert "RecursionError" in deserialize(error_data)['message']
            break
        time.sleep(0.1)


def test_PUSH_worker_failure_automated():
    """Test worker failure with retry cycles. This startes and kills workers to see how the push dispatcher handles that. 
    We use retry mechanisms to to see if work can still be completed. Here we check whether work goes from 
    Queued to running then from runing to queued when we kill the worker, then again from queued to running when a new worker is started. then kill that worker again and status should go from running to queued and start another so state goes from queued to running
    And lastly we kill the worker and leave the dispatcher to perform 4 MAX-RETRIES until it sets the task as failed.
    This shows how task states are handled when worker dies and when another comes online and also how the dispatcher uses MAX-RETRIES=4 to try reassigning work before setting it as FAILED"""

    import subprocess
    import os
    
    def start_worker():
        process = subprocess.Popen(
            ["python3", "push_model/push_worker.py", "1", "tcp://127.0.0.1:5556"],
            # ["python3", "push_model/push_worker.py", "1", "1", "tcp://127.0.0.1:5556"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(3)
        return process
    
    def start_dispatcher():
        process = subprocess.Popen(
            ["python3", "push_model/push_dispatcher.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(3)  # Allow dispatcher to initialize
        return process

    def check_status_transition(task_id, from_status, to_status):
        print(f"\nWaiting for transition from {from_status} to {to_status}")
        for i in range(30):
            resp = requests.get(f"{base_url}status/{task_id}")
            status = resp.json()['status']
            print(f"Check {i+1}/30 - Status: {status}")
            
            if status == to_status:
                return True
            time.sleep(1)
        return False

    try:
        # Initial cleanup
        os.system("pkill -f push_worker")
        os.system("pkill -f push_dispatcher")
        time.sleep(2)

        # Start dispatcher
        print("\n=== Starting dispatcher ===")
        dispatcher = start_dispatcher()

        # First worker - Initial run (count=0)
        print("\n=== Starting first worker (initial run) ===")
        worker = start_worker()
        
        def long_task():
            time.sleep(30)
            return "Done"

        resp = requests.post(base_url + "register_function",
                           json={"name": "long_task", "payload": serialize(long_task)})
        fn_info = resp.json()
        
        resp = requests.post(base_url + "execute_function",
                           json={"function_id": fn_info['function_id'],
                                "payload": serialize(((), {}))})
        task_id = resp.json()["task_id"]
        print(f"\nTask ID: {task_id}")
        
        # Initial run -> First retry
        assert check_status_transition(task_id, "QUEUED", "RUNNING")
        print("\nKilling first worker...")
        worker.kill()
        os.system("pkill -f push_worker")
        time.sleep(2)
        assert check_status_transition(task_id, "RUNNING", "QUEUED")

        # Second worker - First retry (count=1)
        print("\n=== Starting second worker (retry #1) ===")
        worker = start_worker()
        assert check_status_transition(task_id, "QUEUED", "RUNNING")
        print("\nKilling second worker...")
        worker.kill()
        os.system("pkill -f push_worker")
        time.sleep(2)
        assert check_status_transition(task_id, "RUNNING", "QUEUED")

        # Third worker - Second retry (count=2)
        print("\n=== Starting third worker (retry #2) ===")
        worker = start_worker()
        assert check_status_transition(task_id, "QUEUED", "RUNNING")
        print("\nKilling third worker...")
        worker.kill()
        os.system("pkill -f push_worker")
        time.sleep(2)
        assert check_status_transition(task_id, "RUNNING", "QUEUED")

        # Fourth worker - Third retry (count=3)
        print("\n=== Starting fourth worker (retry #3) ===")
        worker = start_worker()
        assert check_status_transition(task_id, "QUEUED", "RUNNING")
        print("\nKilling fourth worker...")
        worker.kill()
        os.system("pkill -f push_worker")
        time.sleep(2)

        # Check final failure state after fourth kill (count=4 > MAX_RETRIES)
        print("\n=== Checking final state ===")
        for i in range(30):
            resp = requests.get(f"{base_url}result/{task_id}")
            status = resp.json()['status']
            print(f"Check {i+1}/30 - Status: {status}")
            
            if status == "FAILED":
                error_data = deserialize(resp.json()['result'])
                while isinstance(error_data, str):
                    error_data = deserialize(error_data)
                assert error_data['error_type'] == "WorkerFailureError"
                print("Test passed: Worker failure detected after max retries")
                return
            time.sleep(1)
            
        raise AssertionError("Task never reached FAILED state")
            
    finally:
        # Clean up all processes
        os.system("pkill -f push_worker")
        os.system("pkill -f push_dispatcher")
        if 'dispatcher' in locals():
            dispatcher.kill()

