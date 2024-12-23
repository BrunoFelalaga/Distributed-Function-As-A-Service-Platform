import requests
import logging
import time
import random
import sys
import os
import uuid


# Add the parent directory to the system path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from serialize import serialize, deserialize  # Custom serialization utilities

# Base URL for the API endpoints
base_url = "http://127.0.0.1:8000/"

# Valid statuses for a task in the system
valid_statuses = ["QUEUED", "RUNNING", "COMPLETED", "FAILED"]


def test_fn_registration_invalid():
    # Test registering a function with invalid (non-serialized) payload
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",  # Function name
                               "payload": "payload"})  # Invalid payload
    # Assert that the API returns HTTP 400 or 500 for invalid input
    assert resp.status_code in [500, 400]


def double(x):
    # A simple function that doubles the input
    return x * 2


def test_fn_registration(): 
    # Serialize the 'double' function for registration
    serialized_fn = serialize(double)

    # Send the serialized function to the API for registration
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",  # Function name
                               "payload": serialized_fn})  # Serialized function
    # Ensure that the response is successful and contains a function ID
    assert resp.status_code in [200, 201]
    assert "function_id" in resp.json()


def test_execute_fn():
    # Register the 'double' function and get its information
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",  # Function name
                               "payload": serialize(double)})  # Serialized function
    fn_info = resp.json()  # Parse the response JSON
    assert "function_id" in fn_info  # Ensure function ID is returned

    # Execute the registered function with a payload
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],  # ID of the registered function
                               "payload": serialize(((2,), {}))})  # Payload to double the number 2

    # Ensure the execution request is accepted
    assert resp.status_code == 200 or resp.status_code == 201
    assert "task_id" in resp.json()  # Ensure a task ID is returned
    task_id = resp.json()["task_id"]  # Extract the task ID

    # Check the status of the task
    resp = requests.get(f"{base_url}status/{task_id}")
    assert resp.status_code == 200  # Ensure the status endpoint is reachable
    assert resp.json()["task_id"] == task_id  # Verify the task ID matches
    assert resp.json()["status"] in valid_statuses  # Ensure the task status is valid


def test_roundtrip():
    # Register the 'double' function
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",  # Function name
                               "payload": serialize(double)})  # Serialized function
    fn_info = resp.json()  # Parse the response JSON

    print("\nINFO:", fn_info)  # Log the function registration info

    # Generate a random number to double
    number = random.randint(0, 10000)

    # Execute the registered 'double' function with the random number
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],  # Registered function ID
                               "payload": serialize(((number,), {}))})  # Payload with the random number

    # Ensure the execution request is accepted
    assert resp.status_code in [200, 201]
    assert "task_id" in resp.json()  # Ensure a task ID is returned

    task_id = resp.json()["task_id"]  # Extract the task ID

    # Poll for the task result with retries
    for i in range(20):
        resp = requests.get(f"{base_url}result/{task_id}")  # Get the task result

        # Ensure the result endpoint is reachable
        assert resp.status_code == 200
        assert resp.json()["task_id"] == task_id  # Verify the task ID matches

        # Check if the task is completed or failed
        if resp.json()['status'] in ["COMPLETED", "FAILED"]:
            logging.warning(f"Task is now in {resp.json()['status']}")  # Log the task status
            s_result = resp.json()  # Parse the result JSON
            logging.warning(s_result)  # Log the raw result
            result = deserialize(s_result['result'])  # Deserialize the result
            result = deserialize(result)
            assert result == number * 2  # Ensure the result matches the expected value
            break  # Exit the loop if task is complete
        time.sleep(0.01)  # Wait before retrying



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
            print("\nalicia starts school in a couple of weeks!!!\n")
            break
        time.sleep(0.1)



def long_running_function(sleep_time):
    time.sleep(sleep_time)
    return sleep_time




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



