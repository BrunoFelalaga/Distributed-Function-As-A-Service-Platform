from typing import Optional
import uuid
import traceback
class FaaSBaseException(Exception):
    """Base exception class for FaaS system"""
    def __init__(self, message: str, task_id: Optional[uuid.UUID] = None):
        self.message = message
        self.task_id = task_id
        super().__init__(self.message)

class FunctionExecutionFailure(FaaSBaseException):
    """Exception for errors during task execution"""
    def __init__(self, message: str, task_id: uuid.UUID, original_error: Exception):
        self.original_error = original_error
        self.traceback = traceback.format_exc()
        super().__init__(f"Task {task_id} failed: {message}\nOriginal error: {str(original_error)}\n{self.traceback}", 
                        task_id)

# class WorkerFailureError(FaaSBaseException):
#     """Exception for worker failures"""
#     def __init__(self, message: str, task_id: uuid.UUID, worker_id: str):
#         self.worker_id = worker_id
#         super().__init__(f"Worker {worker_id} failed while processing task {task_id}: {message}", 
#                         task_id)

# General WorkerFailureError exception for worker-specific failures
class WorkerFailureError(FaaSBaseException):
    """Exception for worker failures"""
    def __init__(self, message: str, task_id: Optional[uuid.UUID] = None, worker_id: Optional[str] = None):
        self.worker_id = worker_id
        super().__init__(
            f"Worker {worker_id} failed while processing task {task_id}: {message}",
            task_id
        )
class DeadlineExceededError(WorkerFailureError):
    """Exception for tasks that exceed their deadline"""
    def __init__(self, task_id: uuid.UUID, worker_id: str, deadline: float, actual_time: float):
        self.deadline = deadline
        self.actual_time = actual_time
        super().__init__(
            f"Task exceeded deadline of {deadline}s (took {actual_time:.2f}s)", 
            task_id,
            worker_id
        )

class SerializationError(FaaSBaseException):
    """Exception for serialization/deserialization errors"""
    def __init__(self, message: str, task_id: Optional[uuid.UUID] = None, operation: str = "unknown"):
        super().__init__(f"Serialization error during {operation}: {message}", task_id)

def handle_task_failure(task_id: uuid.UUID, error: Exception) -> dict:
    """Convert task failures into standardized error response"""
    if isinstance(error, FaaSBaseException):
        error_data = {
            "status": "FAILED",
            "error_type": error.__class__.__name__,
            "message": str(error),
            "task_id": str(task_id)
        }
        
        if isinstance(error, FunctionExecutionFailure):
            error_data["traceback"] = error.traceback
        elif isinstance(error, WorkerFailureError):
            error_data["worker_id"] = error.worker_id
            if isinstance(error, DeadlineExceededError):
                error_data["deadline"] = error.deadline
                error_data["actual_time"] = error.actual_time
                
        return error_data
    
    # Handle unexpected exceptions
    return {
        "status": "FAILED",
        "error_type": "UnexpectedError",
        "message": str(error),
        "task_id": str(task_id)
    }