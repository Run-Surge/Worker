"""Configuration management for the worker node."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class WorkerConfig:
    """Configuration settings for the worker node."""
    
    # Worker identification
    worker_id: str = "worker_001"
    
    # Network settings
    listen_port: int = 50051
    master_address: str = "localhost:50050"
    
    # Resource limits
    max_concurrent_jobs: int = 1
    cpu_cores: Optional[int] = 1  # Auto-detect if None
    memory_mb: Optional[float] = 1024  # Auto-detect if None (1GB = 1024MB)
    
    # Job execution settings
    job_timeout_seconds: int = 300  # 5 minutes
    python_executable: str = "python"
    
    # Data cache settings
    cache_size_limit_mb: float = 2048  # 2GB = 2048MB
    
    # Logging
    log_level: str = "INFO"
    
    # Directories
    temp_dir: str = "./temp/worker_data"
    cache_dir: str = "./temp/worker_cache"
    
    @classmethod
    def from_env(cls) -> 'WorkerConfig':
        """Create configuration from environment variables."""
        return cls(
            worker_id=os.getenv('WORKER_ID', cls.worker_id),
            listen_port=int(os.getenv('WORKER_PORT', cls.listen_port)),
            master_address=os.getenv('MASTER_ADDRESS', cls.master_address),
            max_concurrent_jobs=int(os.getenv('MAX_CONCURRENT_JOBS', cls.max_concurrent_jobs)),
            job_timeout_seconds=int(os.getenv('JOB_TIMEOUT_SECONDS', cls.job_timeout_seconds)),
            log_level=os.getenv('LOG_LEVEL', cls.log_level),
            temp_dir=os.getenv('TEMP_DIR', cls.temp_dir),
            cache_dir=os.getenv('CACHE_DIR', cls.cache_dir),
        )
    
    def ensure_directories(self):
        """Create necessary directories if they don't exist."""
        os.makedirs(self.temp_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True) 