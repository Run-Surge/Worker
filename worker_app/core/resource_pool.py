"""Resource pool management for worker node."""

import psutil
import threading
import time
from typing import Optional
from typing import Dict, Any
from ..utils.logging_setup import setup_logging


class ResourcePool:
    """
    Manages CPU and memory resources for the worker node.
    
    Tracks allocated resources and determines if new tasks can be accepted.
    Currently simulates resource management but returns True for can_accept_task.
    """
    
    def __init__(self, total_cpu_cores: Optional[int] = None, total_memory_bytes: Optional[int] = None):
        # Auto-detect system resources if not provided
        self.total_cpu_cores = total_cpu_cores or psutil.cpu_count()
        self.total_memory_bytes = total_memory_bytes or psutil.virtual_memory().total
        self.logger = setup_logging('INFO')
        # Track allocated resources
        self.allocated_cpu_cores = 0.0
        self.allocated_memory_bytes = 0
        self.lock = threading.Lock()
        
        # Resource allocation history for debugging
        self.allocations: Dict[str, Dict[str, float]] = {}
    
    def can_accept_task(self, required_cpu: float = 1.0, required_memory_bytes: int = 1024*1024*1024) -> bool:
        """
        Check if we have enough resources for a new task.
        
        Args:
            required_cpu: CPU cores needed for the task
            required_memory_bytes: Memory in bytes needed for the task
            
        Returns:
            True if resources are available (currently always True for simulation)
        """
        cpu_available = (self.allocated_cpu_cores + required_cpu) <= self.total_cpu_cores
        memory_available = (self.allocated_memory_bytes + required_memory_bytes) <= self.total_memory_bytes
        
        # For now, always return True (simulation mode)
        # Later: return cpu_available and memory_available
        return True

    def allocate_resources(self, task_id: str, cpu_cores: float = 1.0, memory_bytes: int = 1024*1024*1024) -> bool:
        """
        Reserve resources for a task.
        
        Args:
            task_id: Unique task identifier
            cpu_cores: CPU cores to allocate
            memory_bytes: Memory in bytes to allocate
            
        Returns:
            True if resources were successfully allocated
        """
        #TODO: make it dynamic or remove it
        self.logger.info(f"Allocating resources for task {task_id}")
        with self.lock:
            if self.can_accept_task(cpu_cores, memory_bytes):
                self.allocated_cpu_cores += cpu_cores
                self.allocated_memory_bytes += memory_bytes
                self.allocations[task_id] = {
                    "cpu": cpu_cores,
                    "memory": memory_bytes,
                    "allocated_at": time.time()
                }
                return True
            return False
    
    def release_resources(self, task_id: str) -> bool:
        """
        Free up resources when task completes.
        
        Args:
            task_id: Task identifier to release resources for
            
        Returns:
            True if resources were successfully released
        """
        with self.lock:
            if task_id in self.allocations:
                allocation = self.allocations.pop(task_id)
                self.allocated_cpu_cores = max(0, self.allocated_cpu_cores - allocation["cpu"])
                self.allocated_memory_bytes = max(0, self.allocated_memory_bytes - allocation["memory"])
                return True
            return False
    
    def get_resource_status(self) -> Dict[str, Any]:
        """Get current resource utilization."""
        with self.lock:
            return {
                "cpu_used": self.allocated_cpu_cores,
                "cpu_total": self.total_cpu_cores,
                "cpu_utilization": self.allocated_cpu_cores / self.total_cpu_cores if self.total_cpu_cores > 0 else 0,
                "memory_used_bytes": self.allocated_memory_bytes,
                "memory_total_bytes": self.total_memory_bytes,
                "memory_total_mb": self.total_memory_bytes / (1024*1024),  # For backward compatibility in logs
                "memory_utilization": self.allocated_memory_bytes / self.total_memory_bytes if self.total_memory_bytes > 0 else 0,
                "active_allocations": len(self.allocations),
                "allocations": dict(self.allocations)  # Copy for thread safety
            }
