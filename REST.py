import uuid
import redis
import dill
import sys
import codecs
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from serialize import serialize, deserialize
# Initialize FastAPI application
app = FastAPI()

# Set up Redis client, connecting to default Redis instance on localhost
redis_client = redis.Redis(host='localhost', port=6379, db=0)  # db=0 for default Redis database instance

# Define data models using Pydantic for request and response validation

# Model for registering a function, includes function name and serialized payload
class RegisterFn(BaseModel):
    name: str
    payload: str

# Response model for function registration, returns a UUID for the function
class RegisterFnRep(BaseModel):
    function_id: uuid.UUID

# Model for checking the status of a task, includes task_id and its status
class TaskStatusRep(BaseModel):
    task_id: uuid.UUID
    status: str

# Model for executing a function, includes function_id and parameters as payload
class ExecuteFnReq(BaseModel):
    function_id: uuid.UUID
    payload: str

# Response model for function execution, returns a UUID for the created task
class ExecuteFnRep(BaseModel):
    task_id: uuid.UUID

# Model for retrieving task results, includes task_id, status, and result
class TaskResultRep(BaseModel):
    task_id: uuid.UUID
    status: str
    result: str


def get_keys(redis_client):
    """
    Get all keys from Redis
    """
    return redis_client.keys('*')


@app.post("/register_function", response_model=RegisterFnRep)
async def register_function(regRequest: RegisterFn):
    try:  
        # Verify if the function payload can be successfully deserialized
        deserialize(regRequest.payload)
    except Exception as e:
        # If deserialization fails, return a 400 Bad Request error
        raise HTTPException(status_code=400, detail="Invalid function payload")

    # Generate a unique ID for the function and store its data
    function_uuid = str(uuid.uuid4())
    function_data = {"name": regRequest.name, "payload": regRequest.payload}
    
    # Serialize function data and store in Redis
    serialized_fn_data = serialize(function_data)
    redis_client.set(function_uuid, serialized_fn_data)
    
    # Return the function ID as a response
    return RegisterFnRep(function_id=function_uuid)


@app.post("/execute_function", response_model=ExecuteFnRep)
async def execute_function(execRequest: ExecuteFnReq):
    # Retrieve function data from Redis and validate its existence
    serialized_fn_data = redis_client.get(str(execRequest.function_id))
    if not serialized_fn_data:
        # Return a 404 error if the function ID is not found in Redis
        raise HTTPException(status_code=404, detail="Function not found in DB")
    
    # Deserialize the function data
    function_data = deserialize(serialized_fn_data)

    # Create a new task with a unique ID and mark it as QUEUED
    task_id = str(uuid.uuid4())
    task_data = { 
        "task_id": task_id,
        "fn_payload": function_data['payload'],
        "param_payload": execRequest.payload,
        "status": "QUEUED",
        "result": None
    }

    # Log the task ID
    print(f"main Task ID: {task_id}")
    
    # Serialize task data and store it in Redis
    serialized_task_data = serialize(task_data)
    redis_client.set(task_id, serialized_task_data)

    # Publish the task ID to the "Tasks" channel for worker processing
    redis_client.publish("Tasks", task_id)
    
    # Return the task ID as a response
    return ExecuteFnRep(task_id=task_id)


@app.get("/status/{task_id}", response_model=TaskStatusRep)
async def get_status(task_id: str):
    # Retrieve serialized task data from Redis using task_id
    serialized_task_data = redis_client.get(task_id)
    if not serialized_task_data:
        # Return a 404 error if the task ID is not found in Redis
        raise HTTPException(status_code=404, detail=f"Task with ID: {task_id} not found in DB")

    # Deserialize the task data to access status information
    task_data = deserialize(serialized_task_data)
    task_status = task_data['status']

    # Return the task status as a response
    return TaskStatusRep(task_id=task_id, status=task_status)


@app.get("/result/{task_id}", response_model=TaskResultRep)
async def get_result(task_id: str):
    # Retrieve serialized task data from Redis using task_id
    serialized_task_data = redis_client.get(task_id)

    # Raise a 404 error if the task ID is not found in Redis
    if not serialized_task_data:
        raise HTTPException(status_code=404, detail=f"Task with ID: {task_id} not found in DB{get_keys(redis_client)}")
    
    # Deserialize the task data
    task_data = deserialize(serialized_task_data)
    
    # Handle cases where the task is still in progress
    if task_data['status'] in ['QUEUED', 'RUNNING']:
        # Ensure result is always returned as a string
        
        # Return status with a message indicating the task is in progress
        return TaskResultRep(task_id=task_id, status=task_data['status'], result='Task is still in progress')

    # Serialize the result or the exception before returning
    result = serialize(task_data.get('result', 'No result available'))
    print(task_data['status'], "\nfff\n")
    return TaskResultRep(task_id=task_id, status=task_data['status'], result=result)

if __name__ == "__main__":
    pass
