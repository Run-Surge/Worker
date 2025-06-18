"""Job processing logic for the worker node."""

import threading
import time
import pickle
from typing import Any, Dict, Optional
from dataclasses import dataclass
from enum import Enum

from ..config import Config
from .cache_manager import CacheManager
from ..utils.logging_setup import setup_logging


class JobStatus(Enum):
    """Job execution status."""
    PENDING = "pending"
    FETCHING_FILES = "fetching_files"
    FETCHING_DATA = "fetching_data"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class JobContext:
    """Context information for a running job."""
    job_id: str
    assignment: Any  # JobAssignment proto
    status: JobStatus
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


class JobProcessor:
    """
    Handles the execution lifecycle of individual jobs.
    
    This is currently a stub implementation that simulates job processing.
    TODO: Implement actual Python code execution, file fetching, and result handling.
    """
    
    def __init__(self, config: Config, data_cache: CacheManager, worker_id: str):
        self.config = config
        self.data_cache = data_cache
        self.worker_id = worker_id
        self.logger = setup_logging(worker_id)
    
    def create_job_context(self, job_assignment, cpu_allocated: float = 1.0, memory_allocated: float = 1.0) -> JobContext:
        """Create a new job context from assignment."""
        return JobContext(
            job_id=job_assignment.job_id,
            assignment=job_assignment,
            status=JobStatus.PENDING,
            thread=None,
            start_time=time.time(),
            end_time=None,
            result=None,
            error=None,
            cpu_allocated=cpu_allocated,
            memory_allocated=memory_allocated
        )
    
    def start_job(self, job_context: JobContext, completion_callback) -> bool:
        """
        Start job execution in a separate thread.
        
        Args:
            job_context: Job context to execute
            completion_callback: Function to call when job completes (job_id, success, result, error)
            
        Returns:
            True if job started successfully, False otherwise
        """
        try:
            # Create and start the job thread
            job_thread = threading.Thread(
                target=self._execute_job,
                args=(job_context, completion_callback),
                name=f"job_{job_context.job_id}",
                daemon=True
            )
            
            job_context.thread = job_thread
            job_thread.start()
            
            self.logger.info(f"Started job {job_context.job_id} in thread {job_thread.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start job {job_context.job_id}: {e}")
            job_context.status = JobStatus.FAILED
            job_context.error = str(e)
            return False
    
    def _execute_job(self, job_context: JobContext, completion_callback):
        """Execute the job in the current thread."""
        try:
            self.logger.info(f"Executing job {job_context.job_id}")
            
            # Step 1: Fetch Python file
            job_context.status = JobStatus.FETCHING_FILES
            python_code = self._fetch_python_file(job_context.assignment.python_file)
            
            # Step 2: Fetch required data
            job_context.status = JobStatus.FETCHING_DATA
            required_data = self._fetch_required_data(job_context.assignment.required_data_ids)
            
            # Step 3: Execute the job
            job_context.status = JobStatus.EXECUTING
            result = self._execute_python_code(python_code, required_data)
            
            # Step 4: Handle result
            job_context.result = result
            job_context.status = JobStatus.COMPLETED
            job_context.end_time = time.time()
            
            self.logger.info(f"Job {job_context.job_id} completed successfully")
            completion_callback(job_context.job_id, True, result, None)
            
        except Exception as e:
            self.logger.error(f"Job {job_context.job_id} failed: {e}")
            job_context.status = JobStatus.FAILED
            job_context.error = str(e)
            job_context.end_time = time.time()
            
            completion_callback(job_context.job_id, False, None, str(e))
    
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
    # Dummy job processing
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
    
    def get_job_info(self, job_context: JobContext) -> Dict[str, Any]:
        """Get information about a job."""
        execution_time = None
        if job_context.end_time:
            execution_time = job_context.end_time - job_context.start_time
        elif job_context.status != JobStatus.PENDING:
            execution_time = time.time() - job_context.start_time
        
        return {
            "job_id": job_context.job_id,
            "status": job_context.status.value,
            "start_time": job_context.start_time,
            "end_time": job_context.end_time,
            "execution_time": execution_time,
            "cpu_allocated": job_context.cpu_allocated,
            "memory_allocated": job_context.memory_allocated,
            "error": job_context.error,
            "thread_alive": job_context.thread.is_alive() if job_context.thread else False
        } 