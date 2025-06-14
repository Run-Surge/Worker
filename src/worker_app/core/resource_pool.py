"""Resource pool management for worker node."""

import psutil
import threading
import time
from typing import Optional
from typing import Dict, Any


class ResourcePool:
    """
    Manages CPU and memory resources for the worker node.
    
    Tracks allocated resources and determines if new jobs can be accepted.
    Currently simulates resource management but returns True for can_accept_job.
    """
    
    def __init__(self, total_cpu_cores: Optional[int] = None, total_memory_mb: Optional[float] = None):
        # Auto-detect system resources if not provided
        self.total_cpu_cores = total_cpu_cores or psutil.cpu_count()
        self.total_memory_mb = total_memory_mb or (psutil.virtual_memory().total / (1024**2))
        
        # Track allocated resources
        self.allocated_cpu_cores = 0.0
        self.allocated_memory_mb = 0.0
        self.lock = threading.Lock()
        
        # Resource allocation history for debugging
        self.allocations: Dict[str, Dict[str, float]] = {}
    
    def can_accept_job(self, required_cpu: float = 1.0, required_memory_mb: float = 1.0) -> bool:
        """
        Check if we have enough resources for a new job.
        
        Args:
            required_cpu: CPU cores needed for the job
            required_memory_mb: Memory in MB needed for the job
            
        Returns:
            True if resources are available (currently always True for simulation)
        """
        with self.lock:
            cpu_available = (self.allocated_cpu_cores + required_cpu) <= self.total_cpu_cores
            memory_available = (self.allocated_memory_mb + required_memory_mb) <= self.total_memory_mb
            
            # For now, always return True (simulation mode)
            # Later: return cpu_available and memory_available
            return True
    
    def allocate_resources(self, job_id: str, cpu_cores: float = 1.0, memory_mb: float = 1024) -> bool:
        """
        Reserve resources for a job.
        
        Args:
            job_id: Unique job identifier
            cpu_cores: CPU cores to allocate
            memory_mb: Memory in MB to allocate
            
        Returns:
            True if resources were successfully allocated
        """
        with self.lock:
            if self.can_accept_job(cpu_cores, memory_mb):
                self.allocated_cpu_cores += cpu_cores
                self.allocated_memory_mb += memory_mb
                self.allocations[job_id] = {
                    "cpu": cpu_cores,
                    "memory": memory_mb,
                    "allocated_at": time.time()
                }
                return True
            return False
    
    def release_resources(self, job_id: str) -> bool:
        """
        Free up resources when job completes.
        
        Args:
            job_id: Job identifier to release resources for
            
        Returns:
            True if resources were successfully released
        """
        with self.lock:
            if job_id in self.allocations:
                allocation = self.allocations.pop(job_id)
                self.allocated_cpu_cores = max(0, self.allocated_cpu_cores - allocation["cpu"])
                self.allocated_memory_mb = max(0, self.allocated_memory_mb - allocation["memory"])
                return True
            return False
    
    def get_resource_status(self) -> Dict[str, Any]:
        """Get current resource utilization."""
        with self.lock:
            return {
                "cpu_used": self.allocated_cpu_cores,
                "cpu_total": self.total_cpu_cores,
                "cpu_utilization": self.allocated_cpu_cores / self.total_cpu_cores if self.total_cpu_cores > 0 else 0,
                "memory_used_mb": self.allocated_memory_mb,
                "memory_total_mb": self.total_memory_mb,
                "memory_utilization": self.allocated_memory_mb / self.total_memory_mb if self.total_memory_mb > 0 else 0,
                "active_allocations": len(self.allocations),
                "allocations": dict(self.allocations)  # Copy for thread safety
            }
