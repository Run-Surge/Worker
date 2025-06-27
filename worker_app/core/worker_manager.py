import os
import threading
import time
from typing import Dict, Optional, Tuple, Any
from enum import Enum

from ..config import Config
from .cache_manager import CacheManager, CacheEntry
from .task_processor import TaskProcessor, TaskContext, TaskStatus
from ..utils.logging_setup import setup_logging
from .resource_pool import ResourcePool
from protos.worker_pb2 import TaskAssignment, DataNotification
from protos.common_pb2 import DataMetadata
from .master_client import MasterClient
from ..utils.util import create_data_path
from ..vm.vm import VMTaskExecutor

class WorkerState(Enum):
    """Worker states matching the proto definition."""
    UNSPECIFIED = 0
    IDLE = 1 # Worker is idle and no tasks are running, can accept new tasks
    WORKING = 2 # Worker is currently working on a task, but can accept new tasks
    BUSY = 3 # Worker is currently working on a task, and cannot accept new tasks
    INITIALIZING = 4
    SHUTTING_DOWN = 5
    SHUTDOWN = 6


class WorkerManager:
    """
    Central orchestrator for the worker node.
    
    Manages worker state, task execution, resource allocation, and data caching.
    Acts as the main coordinator between gRPC servicer and task processing.
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.worker_id = config.worker_id   
        self.logger = setup_logging(self.worker_id)
        
        # Worker state
        self.state = WorkerState.INITIALIZING
        self.startup_time = time.time()
        
        # Resource management
        self.resource_pool = ResourcePool(config.cpu_cores, config.memory_bytes)
        
        # Task management
        self.active_tasks: Dict[str, TaskContext] = {}
        self.task_lock = threading.RLock()
        
        # Components
        self.data_cache = CacheManager(config)
        self.master_client = MasterClient(config)
        self.vm_executor = VMTaskExecutor(
            disk_image=config.disk_image,
            memory_bytes=config.memory_bytes,
            cpus=config.cpu_cores,
            vm_startup_timeout=config.vm_startup_timeout,
            shared_folder_host=os.path.abspath(config.shared_dir),
            shared_folder_guest="/mnt/win",
        )
        self.task_processor = TaskProcessor(config, self.data_cache, self.worker_id, self.master_client, self.vm_executor)
        
        # Initialize directories
        config.ensure_directories()
        
        # Complete initialization
        self._initialize_vm()
        self.state = WorkerState.IDLE
        self.logger.info(f"VM state: {self.vm_executor.get_vm_status()}")
        self.logger.info(f"WorkerManager initialized for worker {self.worker_id}")
        self.logger.info(f"Resource pool: {self.resource_pool.total_cpu_cores} CPU cores, "
                        f"{self.resource_pool.total_memory_bytes} bytes memory")

    def _initialize_vm(self):
        if self.config.start_vm_on_startup:
            result = self.vm_executor.launch_vm()
            if not result:
                self.logger.error("Failed to launch VM")
                raise Exception("Failed to launch VM")
        else:
            self.logger.info("Starting VM on startup is disabled, establishing SSH connection")
            self.vm_executor.vm_running = True
            self.vm_executor.establish_ssh_connection()
            self.logger.info("SSH connection established")

    def can_accept_task(self, task_assignment: TaskAssignment) -> Tuple[bool, str]:
        """
        Check if worker can accept a new task.
        
        Args:
            task_assignment: TaskAssignment proto
            
        Returns:
            Tuple of (can_accept, reason)
        """
        # Check worker state
        # if self.state != WorkerState.IDLE:
        #     return False, f"Worker is not idle (current state: {self.state.name})"
        
        # # Check concurrent task limit
        # with self.task_lock:
        #     if len(self.active_tasks) >= self.config.max_concurrent_tasks:
        #         return False, f"Maximum concurrent tasks reached ({self.config.max_concurrent_tasks})"
        
        # # Check resource availability
        # required_cpu = 1.0  # TODO: Extract from task_assignment
        # required_memory = 1.0  # TODO: Extract from task_assignment
        
        # if not self.resource_pool.can_accept_task(required_cpu, required_memory):
        #     return False, "Insufficient CPU or memory resources"
        
        return True, "Task can be accepted"
    
    def _cleanup_task(self, task_id: str) -> bool:
        """
        Cleanup a task after it completes.
        
        Args:
            task_id: Task identifier

        Returns:
            True if task was cleaned up, False otherwise
        """ 
        with self.task_lock:
            if not task_id in self.active_tasks:
                return False
            if len(self.active_tasks) == self.config.max_concurrent_tasks:
                self.state = WorkerState.IDLE
            
            self.active_tasks.pop(task_id)
            self.resource_pool.release_resources(task_id)
            
            if len(self.active_tasks) == 0: 
                self.state = WorkerState.IDLE
            return True

    async def assign_task(self, task_assignment: TaskAssignment) -> Tuple[bool, str]:
        """
        Assign a new task to the worker.
        
        Args:
            task_assignment: TaskAssignment proto
            
        Returns:
            Tuple of (success, message)
        """
        task_id = task_assignment.task_id
        
        # Check if we can accept the task
        self.logger.info(f"Assigning task {task_id}")
        can_accept, reason = self.can_accept_task(task_assignment)
        self.logger.info(f"Can accept task: {can_accept}, reason: {reason}")
        if not can_accept:
            self.logger.warning(f"Cannot accept task {task_id}: {reason}")
            return False, reason
        
        try:
            with self.task_lock:
                # Allocate resources
                if not self.resource_pool.allocate_resources(task_id, cpu_cores=1.0, memory_bytes=1024*1024*1024):
                    return False, "Failed to allocate resources"
                self.logger.info(f"Allocated resources for task {task_id}")
                # Create task context
                task_context = self.task_processor.create_task_context(
                    task_assignment, 
                    cpu_allocated=1.0, 
                    memory_allocated=1.0
                )
                self.logger.info(f"Created task context for task {task_id}")
                # Store task context
                self.active_tasks[task_id] = task_context
                self.logger.info(f"Stored task context for task {task_id}")
                # Update worker state
                if len(self.active_tasks) == self.config.max_concurrent_tasks:
                    self.logger.info(f"Updated worker state to BUSY")
                    self.state = WorkerState.BUSY
                # Start task execution
                success = self.task_processor.start_task(task_context, self._on_task_completed)
                self.logger.info(f"Started task execution for task {task_id}")
                if not success:
                    # Cleanup on failure
                    self._cleanup_task(task_id)
                    return False, "Failed to start task execution"
                
                self.logger.info(f"Task {task_id} assigned and started")
                return True, "Task assigned successfully"
                
        except Exception as e:
            self.logger.error(f"Error assigning task {task_id}: {e}")
            self._cleanup_task(task_id)
            return False, f"Internal error: {str(e)}"
    
    async def _on_task_completed(self, task_assignment: TaskAssignment, success: bool, result: Any, error: Optional[str]):
        """
        Callback function called when a task completes.
        
        Args:
            task_assignment: TaskAssignment proto
            success: True if task completed successfully
            result: Task result if successful
            error: Error message if failed
        """
        task_context = self.active_tasks[task_assignment.task_id]
        self._cleanup_task(task_assignment.task_id)

        if success:
            self.logger.info(f"Task {task_assignment.task_id} completed - Success: {success}")
            #TODO: add output data to cache manager
            self.logger.debug(f"{len(task_assignment.output_data_infos)} output data infos")
            for output_data_info in task_assignment.output_data_infos:
                data_path = create_data_path(
                    self.config.shared_dir,
                    task_assignment.task_id,
                    output_data_info.data_name
                )

                entry = CacheEntry(
                    path=data_path,
                    size_bytes=0, #TODO: get size of data
                )
                self.data_cache.add_cache_entry(output_data_info.data_id, entry)
            self.logger.info(f"adding output data to cache manager")
            #TODO: Report task completion to master
            await self.master_client.task_complete(
                task_id=task_assignment.task_id,
                average_memory_bytes=task_context.average_memory_bytes,
                total_time_elapsed=task_context.total_time_elapsed
            )
            self.logger.info(f"reporting task completion to master")
        else:
            self.logger.error(f"Task {task_assignment.task_id} failed - Error: {error}")
            #TODO: handle this
        
    def get_worker_status(self) -> Dict[str, Any]:
        """Get current worker status information."""
        with self.task_lock:
            current_task_id = ""
            if self.active_tasks:
                # Get the first active task ID
                current_task_id = next(iter(self.active_tasks.keys()))
            
            return {
                "state": self.state.value,
                "current_task_id": current_task_id,
                "active_tasks_count": len(self.active_tasks),
                "uptime_seconds": time.time() - self.startup_time,
                "resource_status": self.resource_pool.get_resource_status(),
                "cache_stats": self.data_cache.get_status()
            }
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific task."""
        with self.task_lock:
            if task_id in self.active_tasks:
                task_context = self.active_tasks[task_id]
                return self.task_processor.get_task_info(task_context)
            return None
    
    def list_active_tasks(self) -> Dict[str, Dict[str, Any]]:
        """List all active tasks with their status."""
        with self.task_lock:
            return {
                task_id: self.task_processor.get_task_info(task_context)
                for task_id, task_context in self.active_tasks.items()
            }
    

    def notify_data(self, data_notification: DataNotification) -> Tuple[bool, str]:
        """Notify the worker that data is available."""
        self.logger.debug(f"NotifyData called for data {data_notification.data_id}")
        self.logger.debug(f"Data notification: {data_notification}")
        task = self.active_tasks.get(data_notification.task_id)
        if not task:
            self.logger.warning(f"Task {data_notification.task_id} not found")
            return False, f"Task {data_notification.task_id} not found"
        
        is_on_master = data_notification.ip_address == self.config.master_ip_address \
            and data_notification.port == self.config.master_port

        self.logger.info(f"data {data_notification.data_id} is_on_master: {is_on_master}")
        #TODO: fix this, ip and port shouldn't be passed, we should get it from token
        task.required_data_status[data_notification.data_id].outsite_status = DataMetadata(
            data_id=data_notification.data_id,
            data_name=data_notification.data_name,
            ip_address=data_notification.ip_address,
            port=data_notification.port,
            hash=data_notification.hash,
            is_on_master=is_on_master,
            task_id=data_notification.task_id,
            is_zipped=data_notification.is_zipped
        )

        #TODO: download file from here instead of having a thread wait for it
        self.logger.debug(f"Data notification processed successfully for data {data_notification.data_id}")
        return True, f"Data notification processed successfully for data {data_notification.data_id}"

    def shutdown(self):
        """Gracefully shutdown the worker."""
        self.logger.info("Worker shutdown initiated")
        self.state = WorkerState.SHUTTING_DOWN
        self.vm_executor.stop_vm()
        # Kill all OS processes spawned by this worker
        import psutil
        current_process = psutil.Process()
        children = current_process.children(recursive=True)
        print(f"Children: {children}")
        with open("children.txt", "w") as f:
                f.write(f"current process: {current_process.pid}\n")
                f.write(f"children: {children}\n")
        for child in children:
            self.logger.info(f"Terminating process {child.pid}")
            try:
                child.terminate()  # Try graceful termination first
                try:
                    child.wait(timeout=5)  # Wait up to 5 seconds
                except psutil.TimeoutExpired:
                    self.logger.warning(f"Process {child.pid} did not terminate gracefully, forcing kill")
                    child.kill()  # Force kill if process doesn't respond to terminate
            except psutil.NoSuchProcess:
                pass  # Process already terminated
            except Exception as e:
                self.logger.error(f"Error killing process {child.pid}: {e}")
        # TODO: Wait for active tasks to complete or terminate them
        # TODO: Cleanup resources
        # TODO: Notify master of shutdown
        
        self.logger.info("Worker shutdown completed") 
        