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
from ..vm.vm import VMTaskExecutor
from ..utils.util import format_bytes

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
    start_time: Optional[float]
    end_time: Optional[float]
    result: Optional[Any]
    error: Optional[str]
    cpu_allocated: float
    memory_allocated: float
    pid: Optional[int]
    task_timeout: Optional[float] = 7200 # seconds
    memory_usage_total: Optional[float] = 0
    total_time_elapsed: Optional[float] = 0
    current_memory_usage: Optional[int] = 0
    average_memory_bytes: Optional[int] = 0
    def __post_init__(self):
        if self.thread is None:
            self.thread = None


class TaskProcessor:
    """
    Handles the execution lifecycle of individual tasks.
    
    This is currently a stub implementation that simulates task processing.
    TODO: Implement actual Python code execution, file fetching, and result handling.
    """
    
    def __init__(self, config: Config, data_cache: CacheManager, worker_id: str, master_client: MasterClient, vm_executor: VMTaskExecutor):
        self.config = config
        self.data_cache = data_cache
        self.worker_id = worker_id
        self.master_client = master_client
        self.vm_executor = vm_executor
        self.logger = setup_logging("task_processor", config.log_level)
    
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
            start_time=None,
            end_time=None,
            result=None,
            error=None,
            cpu_allocated=cpu_allocated,
            memory_allocated=memory_allocated,
            pid=None
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
        linux_file_path = f'/mnt/win/{str(task_id)}/{file_name}'
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        try:
            with open(file_path, "wb") as f:
                f.write(python_script)
        except Exception as e:
            self.logger.error(f"Failed to store Python script: {e}")
            raise

        return file_path, linux_file_path

    def _execute_task(self, task_context: TaskContext, completion_callback):
        """Execute the task in the current thread."""
        #TODO: just turn this into an async call and remove the thread, the thread doesn't add anything
        with asyncio.Runner() as runner:
            runner.run(self._execute_task_async(task_context, completion_callback))

    async def _wait_for_python_code_to_finish(self, pid: int, task_context: TaskContext) -> bool:
        """
        Wait for the python code to finish executing.
        """
        self.logger.debug(f"Waiting for python code to finish executing with PID: {pid}")
        timeout = task_context.task_timeout
        start_time = time.time()
        while True:
            await asyncio.sleep(1)
            memory_usage = self.vm_executor.get_process_memory_usage(pid, str(task_context.task_id))
            if memory_usage is None:
                task_context.total_time_elapsed = time.time() - start_time
                task_context.average_memory_bytes = int(task_context.memory_usage_total / task_context.total_time_elapsed)
                self.logger.debug(f"Task {task_context.task_id} total time elapsed: {task_context.total_time_elapsed}")
                self.logger.debug(f"Task {task_context.task_id} average memory usage: {format_bytes(task_context.average_memory_bytes)}")
                self.logger.debug(f"Task {task_context.task_id} memory usage total: {format_bytes(task_context.memory_usage_total)}")
                return True # process has closed
            
            self.logger.debug(f"Memory usage: {format_bytes(memory_usage)}")
            task_context.memory_usage_total += memory_usage
            task_context.current_memory_usage = memory_usage
            if time.time() - task_context.start_time > timeout:
                self.logger.error(f"Task {task_context.task_id} timed out")
                self.vm_executor.kill_process(pid)
                return False # timeout

    async def _execute_task_async(self, task_context: TaskContext, completion_callback):
        try:
            self.logger.info(f"Executing task {task_context.task_id}")
            self.logger.debug(f"Task {task_context.task_id} task context: {task_context}")
            # Step 1: Fetch Python script
            task_assignment = task_context.task_assignment
            task_context.status = TaskStatus.FETCHING_FILES
            python_script_path, linux_python_script_path = self._store_python_script(
                task_assignment.python_file,
                task_context.task_id,
                task_assignment.python_file_name
                )


            # Step 2: Fetch required data
            task_context.status = TaskStatus.FETCHING_DATA
            await self._fetch_required_data(task_context.required_data_status, task_context)
            
            # Step 3: Execute the task
            task_context.status = TaskStatus.EXECUTING
            result, pid = await self._execute_python_code(task_assignment, linux_python_script_path, task_context)
            task_context.pid = pid

            await self.master_client.task_start(task_context.task_id, task_context.start_time)

            
            
            #TODO: This is a hack to wait for the python code to finish executing
            #TODO: check memory usage using ssh connection, will be used to check if the python code is finished executing
            self.logger.debug(f"sleeping for 5 seconds to wait for python code to finish executing")
            await self._wait_for_python_code_to_finish(pid, task_context)
            
            self.logger.debug(f"Python code finished executing")
            
            # Step 4: Handle result
            task_context.result = result
            task_context.status = TaskStatus.COMPLETED
            task_context.end_time = time.time()
            
            self.logger.info(f"Task {task_context.task_id} completed successfully")
            await completion_callback(task_context.task_assignment, True, result, None)
            
        except Exception as e:
            self.logger.error(f"Task {task_context.task_id} failed: {e}")
            task_context.status = TaskStatus.FAILED
            task_context.error = str(e)
            task_context.end_time = time.time()
            
            await completion_callback(task_context.task_assignment, False, None, str(e))
    
    
    async def _fetch_required_data(self, required_data_status: dict[str, TaskDataInfo], task_context: TaskContext):
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
                        self.logger.debug(f"Waiting for data {data_id} in task {task_context.task_id} to be ready")
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

    
    async def _execute_python_code(self, task_assignment: TaskAssignment, python_script_path: str, task_context: TaskContext) -> Any:
        """
        Execute the Python code with the provided data.
        
        TODO: invoke VM to execute the python code using the python script path. (it should know data file names, path by itself)
        """
        self.logger.debug(f"Executing Python code in path {python_script_path}")
        task_context.start_time = time.time()
        pid = self.vm_executor.execute_script(python_script_path)
       
        if pid is None:
            self.logger.error(f"Failed to execute Python code")
            raise Exception("Failed to execute Python code")
        
        self.logger.debug(f"Python code executed with PID: {pid}")
        
        # Simulate processing result
        #TODO: get the result from the python code
        result = {
            "status": "success",
            "processed_data": f"Processed: {task_assignment.required_data_ids}",
            "execution_time": 1.0,
            "worker_id": self.worker_id
        }

        return result, pid
                
    
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