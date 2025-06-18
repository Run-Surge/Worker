"""Central worker manager with resource pool and job orchestration."""

import threading
import time
from typing import Dict, Optional, Tuple, Any
from enum import Enum

from ..config import Config
from .cache_manager import CacheManager
from .job_processor import JobProcessor, JobContext, JobStatus
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
    
    Manages worker state, job execution, resource allocation, and data caching.
    Acts as the main coordinator between gRPC servicer and job processing.
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
        
        # Job management
        self.active_jobs: Dict[str, JobContext] = {}
        self.job_lock = threading.RLock()
        
        # Components
        self.data_cache = CacheManager(config)
        self.job_processor = JobProcessor(config, self.data_cache, self.worker_id)
        
        # Initialize directories
        config.ensure_directories()
        
        # Complete initialization
        self.state = WorkerState.IDLE
        self.logger.info(f"WorkerManager initialized for worker {self.worker_id}")
        self.logger.info(f"Resource pool: {self.resource_pool.total_cpu_cores} CPU cores, "
                        f"{self.resource_pool.total_memory_mb:.1f}MB memory")
    
    def can_accept_job(self, job_assignment) -> Tuple[bool, str]:
        """
        Check if worker can accept a new job.
        
        Args:
            job_assignment: JobAssignment proto
            
        Returns:
            Tuple of (can_accept, reason)
        """
        # Check worker state
        if self.state != WorkerState.IDLE:
            return False, f"Worker is not idle (current state: {self.state.name})"
        
        # Check concurrent job limit
        with self.job_lock:
            if len(self.active_jobs) >= self.config.max_concurrent_jobs:
                return False, f"Maximum concurrent jobs reached ({self.config.max_concurrent_jobs})"
        
        # Check resource availability
        required_cpu = 1.0  # TODO: Extract from job_assignment
        required_memory = 1.0  # TODO: Extract from job_assignment
        
        if not self.resource_pool.can_accept_job(required_cpu, required_memory):
            return False, "Insufficient CPU or memory resources"
        
        return True, "Job can be accepted"
    
    def assign_job(self, job_assignment) -> Tuple[bool, str]:
        """
        Assign a new job to the worker.
        
        Args:
            job_assignment: JobAssignment proto
            
        Returns:
            Tuple of (success, message)
        """
        job_id = job_assignment.job_id
        
        # Check if we can accept the job
        can_accept, reason = self.can_accept_job(job_assignment)
        if not can_accept:
            self.logger.warning(f"Cannot accept job {job_id}: {reason}")
            return False, reason
        
        try:
            with self.job_lock:
                # Allocate resources
                if not self.resource_pool.allocate_resources(job_id, cpu_cores=1.0, memory_mb=1024):
                    return False, "Failed to allocate resources"
                
                # Create job context
                job_context = self.job_processor.create_job_context(
                    job_assignment, 
                    cpu_allocated=1.0, 
                    memory_allocated=1.0
                )
                
                # Store job context
                self.active_jobs[job_id] = job_context
                
                # Update worker state
                self.state = WorkerState.BUSY
                
                # Start job execution
                success = self.job_processor.start_job(job_context, self._on_job_completed)
                
                if not success:
                    # Cleanup on failure
                    self.active_jobs.pop(job_id, None)
                    self.resource_pool.release_resources(job_id)
                    if len(self.active_jobs) == 0:
                        self.state = WorkerState.IDLE
                    return False, "Failed to start job execution"
                
                self.logger.info(f"Job {job_id} assigned and started")
                return True, "Job assigned successfully"
                
        except Exception as e:
            self.logger.error(f"Error assigning job {job_id}: {e}")
            # Cleanup
            self.active_jobs.pop(job_id, None)
            self.resource_pool.release_resources(job_id)
            if len(self.active_jobs) == 0:
                self.state = WorkerState.IDLE
            return False, f"Internal error: {str(e)}"
    
    def _on_job_completed(self, job_id: str, success: bool, result: Any, error: Optional[str]):
        """
        Callback function called when a job completes.
        
        Args:
            job_id: Job identifier
            success: True if job completed successfully
            result: Job result if successful
            error: Error message if failed
        """
        self.logger.info(f"Job {job_id} completed - Success: {success}")
        
        try:
            with self.job_lock:
                # Release resources
                self.resource_pool.release_resources(job_id)
                
                # Update job context if it exists
                if job_id in self.active_jobs:
                    job_context = self.active_jobs[job_id]
                    if success:
                        job_context.status = JobStatus.COMPLETED
                        job_context.result = result
                    else:
                        job_context.status = JobStatus.FAILED
                        job_context.error = error
                    
                    job_context.end_time = time.time()
                
                # TODO: Report job completion to master
                # TODO: Handle produced data if any
                
                # Update worker state if no more active jobs
                if len(self.active_jobs) == 0:
                    self.state = WorkerState.IDLE
                    self.logger.info("Worker returned to IDLE state")
                
        except Exception as e:
            self.logger.error(f"Error in job completion callback for {job_id}: {e}")
    
    def get_worker_status(self) -> Dict[str, Any]:
        """Get current worker status information."""
        with self.job_lock:
            current_job_id = ""
            if self.active_jobs:
                # Get the first active job ID
                current_job_id = next(iter(self.active_jobs.keys()))
            
            return {
                "state": self.state.value,
                "current_job_id": current_job_id,
                "active_jobs_count": len(self.active_jobs),
                "uptime_seconds": time.time() - self.startup_time,
                "resource_status": self.resource_pool.get_resource_status(),
                "cache_stats": self.data_cache.get_status()
            }
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific job."""
        with self.job_lock:
            if job_id in self.active_jobs:
                job_context = self.active_jobs[job_id]
                return self.job_processor.get_job_info(job_context)
            return None
    
    def list_active_jobs(self) -> Dict[str, Dict[str, Any]]:
        """List all active jobs with their status."""
        with self.job_lock:
            return {
                job_id: self.job_processor.get_job_info(job_context)
                for job_id, job_context in self.active_jobs.items()
            }
    
    def shutdown(self):
        """Gracefully shutdown the worker."""
        self.logger.info("Worker shutdown initiated")
        self.state = WorkerState.SHUTTING_DOWN
        
        # TODO: Wait for active jobs to complete or terminate them
        # TODO: Cleanup resources
        # TODO: Notify master of shutdown
        
        self.logger.info("Worker shutdown completed") 