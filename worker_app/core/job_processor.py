import threading
import time
import pickle
from typing import Any, Dict, Optional
from dataclasses import dataclass
from enum import Enum

from ..config import Config
from .cache_manager import CacheManager
from ..utils.logging_setup import setup_logging


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    FETCHING_FILES = "fetching_files"
    FETCHING_DATA = "fetching_data"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class TaskContext:
    """Context information for a running task."""
    task_id: str
    assignment: Any  # TaskAssignment proto
    status: TaskStatus
    thread: Optional[threading.Thread]
    start_time: float
    end_time: Optional[float]
    result: Optional[Any]
    error: Optional[str]
    cpu_allocated: float
    memory_allocated: float
    
    def __post_init__(self):
        if self.thread is None:
            self.thread = None


class TaskProcessor:
    """
    Handles the execution lifecycle of individual tasks.
    
    This is currently a stub implementation that simulates task processing.
    TODO: Implement actual Python code execution, file fetching, and result handling.
    """
    
    def __init__(self, config: Config, data_cache: CacheManager, worker_id: str):
        self.config = config
        self.data_cache = data_cache
        self.worker_id = worker_id
        self.logger = setup_logging(worker_id)
    
    def create_task_context(self, task_assignment, cpu_allocated: float = 1.0, memory_allocated: float = 1.0) -> TaskContext:
        """Create a new task context from assignment."""
        return TaskContext(
            task_id=task_assignment.task_id,
            assignment=task_assignment,
            status=TaskStatus.PENDING,
            thread=None,
            start_time=time.time(),
            end_time=None,
            result=None,
            error=None,
            cpu_allocated=cpu_allocated,
            memory_allocated=memory_allocated
        )
    
    def start_task(self, task_context: TaskContext, completion_callback) -> bool:
        """
        Start task execution in a separate thread.
        
        Args:
            task_context: Task context to execute
            completion_callback: Function to call when task completes (task_id, success, result, error)
            
        Returns:
            True if task started successfully, False otherwise
        """
        try:
            # Create and start the task thread
            task_thread = threading.Thread(
                target=self._execute_task,
                args=(task_context, completion_callback),
                name=f"task_{task_context.task_id}",
                daemon=True
            )
            
            task_context.thread = task_thread
            task_thread.start()
            
            self.logger.info(f"Started task {task_context.task_id} in thread {task_thread.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start task {task_context.task_id}: {e}")
            task_context.status = TaskStatus.FAILED
            task_context.error = str(e)
            return False
    
    def _execute_task(self, task_context: TaskContext, completion_callback):
        """Execute the task in the current thread."""
        try:
            self.logger.info(f"Executing task {task_context.task_id}")
            
            # Step 1: Fetch Python file
            task_context.status = TaskStatus.FETCHING_FILES
            python_code = self._fetch_python_file(task_context.assignment.python_file)
            
            # Step 2: Fetch required data
            task_context.status = TaskStatus.FETCHING_DATA
            required_data = self._fetch_required_data(task_context.assignment.required_data_ids)
            
            # Step 3: Execute the task
            task_context.status = TaskStatus.EXECUTING
            result = self._execute_python_code(python_code, required_data)
            
            # Step 4: Handle result
            task_context.result = result
            task_context.status = TaskStatus.COMPLETED
            task_context.end_time = time.time()
            
            self.logger.info(f"Task {task_context.task_id} completed successfully")
            completion_callback(task_context.task_id, True, result, None)
            
        except Exception as e:
            self.logger.error(f"Task {task_context.task_id} failed: {e}")
            task_context.status = TaskStatus.FAILED
            task_context.error = str(e)
            task_context.end_time = time.time()
            
            completion_callback(task_context.task_id, False, None, str(e))
    
    def _fetch_python_file(self, file_identifier) -> str:
        """
        Fetch Python file from master or cache.
        
        TODO: Implement actual gRPC streaming to fetch files.
        """
        self.logger.debug(f"Fetching Python file: {file_identifier.id}")
        
        # Simulate file fetching
        time.sleep(0.1)  # Simulate network delay
        
        # For now, return a dummy Python code
        return """
def process_data(data):
    # Dummy task processing
    import time
    time.sleep(1)  # Simulate work
    return {"processed": True, "input_data": str(data)}
"""
    
    def _fetch_required_data(self, data_identifier) -> Any:
        """
        Fetch required data from cache or other workers/master.
        
        TODO: Implement actual gRPC streaming to fetch data.
        """
        data_id = data_identifier.id
        self.logger.debug(f"Fetching required data: {data_id}")
        
        # Check cache first
        cached_data = self.data_cache.get(data_id)
        if cached_data is not None:
            self.logger.debug(f"Data {data_id} found in cache")
            return cached_data
        
        # Simulate data fetching from remote source
        time.sleep(0.2)  # Simulate network delay
        
        # For now, return dummy data
        dummy_data = {"sample_data": f"data_for_{data_id}", "timestamp": time.time()}
        
        # Cache the fetched data
        self.data_cache.put(data_id, dummy_data)
        
        return dummy_data
    
    def _execute_python_code(self, python_code: str, input_data: Any) -> Any:
        """
        Execute the Python code with the provided data.
        
        TODO: Implement safe execution environment (subprocess or container).
        """
        self.logger.debug("Executing Python code")
        
        try:
            # For now, simulate code execution
            time.sleep(1)  # Simulate computation time
            
            # In a real implementation, this would:
            # 1. Create a safe execution environment
            # 2. Execute the Python code with input_data
            # 3. Capture and return the result
            
            # Simulate processing result
            result = {
                "status": "success",
                "processed_data": f"Processed: {input_data}",
                "execution_time": 1.0,
                "worker_id": self.worker_id
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Code execution failed: {e}")
            raise
    
    def get_task_info(self, task_context: TaskContext) -> Dict[str, Any]:
        """Get information about a task."""
        execution_time = None
        if task_context.end_time:
            execution_time = task_context.end_time - task_context.start_time
        elif task_context.status != TaskStatus.PENDING:
            execution_time = time.time() - task_context.start_time
        
        return {
            "task_id": task_context.task_id,
            "status": task_context.status.value,
            "start_time": task_context.start_time,
            "end_time": task_context.end_time,
            "execution_time": execution_time,
            "cpu_allocated": task_context.cpu_allocated,
            "memory_allocated": task_context.memory_allocated,
            "error": task_context.error,
            "thread_alive": task_context.thread.is_alive() if task_context.thread else False
        } 