import threading
import time
from typing import Dict, Optional, Tuple, Any
from enum import Enum

from ..config import Config
from .cache_manager import CacheManager
from .job_processor import TaskProcessor, TaskContext, TaskStatus
from ..utils.logging_setup import setup_logging
from .resource_pool import ResourcePool

class WorkerState(Enum):
    """Worker states matching the proto definition."""
    UNSPECIFIED = 0
    IDLE = 1
    BUSY = 2
    INITIALIZING = 3
    SHUTTING_DOWN = 4


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
        self.resource_pool = ResourcePool(config.cpu_cores, config.memory_mb)
        
        # Task management
        self.active_tasks: Dict[str, TaskContext] = {}
        self.task_lock = threading.RLock()
        
        # Components
        self.data_cache = CacheManager(config)
        self.task_processor = TaskProcessor(config, self.data_cache, self.worker_id)
        
        # Initialize directories
        config.ensure_directories()
        
        # Complete initialization
        self.state = WorkerState.IDLE
        self.logger.info(f"WorkerManager initialized for worker {self.worker_id}")
        self.logger.info(f"Resource pool: {self.resource_pool.total_cpu_cores} CPU cores, "
                        f"{self.resource_pool.total_memory_mb:.1f}MB memory")
    
    def can_accept_task(self, task_assignment) -> Tuple[bool, str]:
        """
        Check if worker can accept a new task.
        
        Args:
            task_assignment: TaskAssignment proto
            
        Returns:
            Tuple of (can_accept, reason)
        """
        # Check worker state
        if self.state != WorkerState.IDLE:
            return False, f"Worker is not idle (current state: {self.state.name})"
        
        # Check concurrent task limit
        with self.task_lock:
            if len(self.active_tasks) >= self.config.max_concurrent_tasks:
                return False, f"Maximum concurrent tasks reached ({self.config.max_concurrent_tasks})"
        
        # Check resource availability
        required_cpu = 1.0  # TODO: Extract from task_assignment
        required_memory = 1.0  # TODO: Extract from task_assignment
        
        if not self.resource_pool.can_accept_task(required_cpu, required_memory):
            return False, "Insufficient CPU or memory resources"
        
        return True, "Task can be accepted"
    
    def assign_task(self, task_assignment) -> Tuple[bool, str]:
        """
        Assign a new task to the worker.
        
        Args:
            task_assignment: TaskAssignment proto
            
        Returns:
            Tuple of (success, message)
        """
        task_id = task_assignment.task_id
        
        # Check if we can accept the task
        can_accept, reason = self.can_accept_task(task_assignment)
        if not can_accept:
            self.logger.warning(f"Cannot accept task {task_id}: {reason}")
            return False, reason
        
        try:
            with self.task_lock:
                # Allocate resources
                if not self.resource_pool.allocate_resources(task_id, cpu_cores=1.0, memory_mb=1024):
                    return False, "Failed to allocate resources"
                
                # Create task context
                task_context = self.task_processor.create_task_context(
                    task_assignment, 
                    cpu_allocated=1.0, 
                    memory_allocated=1.0
                )
                
                # Store task context
                self.active_tasks[task_id] = task_context
                
                # Update worker state
                self.state = WorkerState.BUSY
                
                # Start task execution
                success = self.task_processor.start_task(task_context, self._on_task_completed)
                
                if not success:
                    # Cleanup on failure
                    self.active_tasks.pop(task_id, None)
                    self.resource_pool.release_resources(task_id)
                    if len(self.active_tasks) == 0:
                        self.state = WorkerState.IDLE
                    return False, "Failed to start task execution"
                
                self.logger.info(f"Task {task_id} assigned and started")
                return True, "Task assigned successfully"
                
        except Exception as e:
            self.logger.error(f"Error assigning task {task_id}: {e}")
            # Cleanup
            self.active_tasks.pop(task_id, None)
            self.resource_pool.release_resources(task_id)
            if len(self.active_tasks) == 0:
                self.state = WorkerState.IDLE
            return False, f"Internal error: {str(e)}"
    
    def _on_task_completed(self, task_id: str, success: bool, result: Any, error: Optional[str]):
        """
        Callback function called when a task completes.
        
        Args:
            task_id: Task identifier
            success: True if task completed successfully
            result: Task result if successful
            error: Error message if failed
        """
        self.logger.info(f"Task {task_id} completed - Success: {success}")
        
        try:
            with self.task_lock:
                # Release resources
                self.resource_pool.release_resources(task_id)
                
                # Update task context if it exists
                if task_id in self.active_tasks: 
                    task_context = self.active_tasks[task_id]
                    if success:
                        task_context.status = TaskStatus.COMPLETED
                        task_context.result = result
                    else:
                        task_context.status = TaskStatus.FAILED
                        task_context.error = error
                    
                    task_context.end_time = time.time()
                
                # TODO: Report task completion to master
                # TODO: Handle produced data if any
                
                # Update worker state if no more active tasks
                if len(self.active_tasks) == 0:
                    self.state = WorkerState.IDLE
                    self.logger.info("Worker returned to IDLE state")
                
        except Exception as e:
            self.logger.error(f"Error in task completion callback for {task_id}: {e}")
    
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
    
    def shutdown(self):
        """Gracefully shutdown the worker."""
        self.logger.info("Worker shutdown initiated")
        self.state = WorkerState.SHUTTING_DOWN
        
        # TODO: Wait for active tasks to complete or terminate them
        # TODO: Cleanup resources
        # TODO: Notify master of shutdown
        
        self.logger.info("Worker shutdown completed") 