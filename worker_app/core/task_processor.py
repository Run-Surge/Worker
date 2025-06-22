import os
import asyncio
import threading
import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

from protos import common_pb2
from protos.worker_pb2 import TaskAssignment

from ..config import Config
from .cache_manager import CacheManager
from ..utils.logging_setup import setup_logging
from .master_client import MasterClient
import subprocess
import sys

class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    FETCHING_FILES = "fetching_files"
    FETCHING_DATA = "fetching_data"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class TaskDataInfo:
    outsite_status: common_pb2.DataMetadata | None=None
    is_downloaded: bool=False
    data_path: str | None=None

@dataclass
class TaskContext:
    """Context information for a running task."""
    task_id: int
    task_assignment: TaskAssignment  # TaskAssignment proto
    required_data_status: dict[int, TaskDataInfo]
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
    
    def __init__(self, config: Config, data_cache: CacheManager, worker_id: str, master_client: MasterClient):
        self.config = config
        self.data_cache = data_cache
        self.worker_id = worker_id
        self.master_client = master_client
        self.logger = setup_logging(config.log_level)
    
    def create_task_context(self, task_assignment: TaskAssignment, cpu_allocated: float = 1.0, memory_allocated: float = 1.0) -> TaskContext:
        """Create a new task context from assignment."""
        
        required_data_status = {}
        for data_id in task_assignment.required_data_ids:
            required_data_status[data_id] = TaskDataInfo()
        
        
        return TaskContext(
            task_id=task_assignment.task_id,
            task_assignment=task_assignment,
            required_data_status=required_data_status,
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
            self.logger.info(f"Starting task {task_context.task_id} in a new thread")
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
    
    def _store_python_script(self, python_script: str, task_id: int, file_name: str) ->str:
        """
        Store the Python script in the shared folder.
        """
        self.logger.debug(f"Storing Python script: {file_name}")
        file_path = os.path.join(self.config.cache_dir, str(task_id), file_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        try:
            with open(file_path, "wb") as f:
                f.write(python_script)
        except Exception as e:
            self.logger.error(f"Failed to store Python script: {e}")
            raise

        return file_path

    def _execute_task(self, task_context: TaskContext, completion_callback):
        """Execute the task in the current thread."""
        #TODO: just turn this into an async call and remove the thread, the thread doesn't add anything
        with asyncio.Runner() as runner:
            runner.run(self._execute_task_async(task_context, completion_callback))

    async def _execute_task_async(self, task_context: TaskContext, completion_callback):
        try:
            self.logger.info(f"Executing task {task_context.task_id}")
            
            # Step 1: Fetch Python script
            task_assignment = task_context.task_assignment
            task_context.status = TaskStatus.FETCHING_FILES
            python_script_path = self._store_python_script(
                task_assignment.python_file,
                task_context.task_id,
                task_assignment.python_file_name
                )

            
            # Step 2: Fetch required data
            task_context.status = TaskStatus.FETCHING_DATA
            await self._fetch_required_data(task_context.required_data_status)
            
            # Step 3: Execute the task
            task_context.status = TaskStatus.EXECUTING
            result = await self._execute_python_code(task_assignment, python_script_path)
            
            # Step 4: Handle result
            task_context.result = result
            task_context.status = TaskStatus.COMPLETED
            task_context.end_time = time.time()
            
            self.logger.info(f"Task {task_context.task_id} completed successfully")
            completion_callback(task_context.task_assignment, True, result, None)
            
        except Exception as e:
            self.logger.error(f"Task {task_context.task_id} failed: {e}")
            task_context.status = TaskStatus.FAILED
            task_context.error = str(e)
            task_context.end_time = time.time()
            
            completion_callback(task_context.task_assignment, False, None, str(e))
    
    
    async def _fetch_required_data(self, required_data_status: dict[str, TaskDataInfo]):
        """
        Fetch required data from cache or other workers/master.
        This waits for a notification from the master that the data is ready.

        TODO: Implement actual gRPC streaming to fetch data.
        """
        self.logger.debug(f"Fetching required data: {required_data_status}")
        try:
            cnt = 0
            while len(required_data_status) != cnt:
                has_downloaded = False
                for data_id, data_info in required_data_status.items():
                    if data_info.is_downloaded:
                        continue
                    
                    if data_info.outsite_status is None:
                        self.logger.debug(f"Waiting for data {data_id} to be ready")
                        continue
                    
                    self.logger.debug(f"Streaming data {data_id} from {data_info.outsite_status.ip_address}:{data_info.outsite_status.port}")
                    data_path = await self.master_client.stream_data(data_info.outsite_status)
                    data_info.is_downloaded = True
                    data_info.data_path = data_path
                    cnt += 1
                    has_downloaded = True

                if not has_downloaded:
                    self.logger.debug(f"Waiting for data to be ready, sleeping for 5 seconds")
                    await asyncio.sleep(5)
                    continue


            self.logger.debug(f"All data fetched")

                    
                
        except Exception as e:
            self.logger.error(f"Failed to fetch required data: {e}")
            raise

    
    async def _execute_python_code(self, task_assignment: TaskAssignment, python_script_path: str) -> Any:
        """
        Execute the Python code with the provided data.
        
        TODO: invoke VM to execute the python code using the python script path. (it should know data file names, path by itself)
        """
        self.logger.debug("Executing Python code")
        
        try:
            # For now, simulate code execution
            time.sleep(1)  # Simulate computation time
            
            # In a real implementation, this would:
            # 1. Create a safe execution environment
            # 2. Execute the Python code with input_data
            # 3. Capture and return the result
            
            def test_run_python_file(script_path: str):
                # Ensure we're in the correct directory
                original_dir = os.getcwd()
                os.chdir(os.path.dirname(script_path))

                try:
                    # Run the Python file and capture output
                    self.logger.debug(f"Running Python file: {script_path}")
                    self.logger.debug(f"Current directory: {os.getcwd()}")
                    result = subprocess.run([sys.executable, os.path.basename(script_path)], 
                                        capture_output=True,
                                        text=True)
                    
                    # Check the output contains the expected text
                    self.logger.info(f"Result: {result.stdout}, length: {len(result.stdout)}")

                finally:
                    # Restore original directory
                    os.chdir(original_dir)

            test_run_python_file(python_script_path)
            # Simulate processing result
            result = {
                "status": "success",
                "processed_data": f"Processed: {task_assignment.required_data_ids}",
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