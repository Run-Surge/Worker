"""Configuration management for the worker node."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuration settings for the worker node."""
    
    # Worker identification
    worker_id: str = "worker_001"
    
    # Network settings
    listen_port: int = 50000
    master_address: str = "10.10.10.213:12345"
    master_ip_address: str = "10.10.10.213"
    master_port: int = 12345
    
    # Resource limits
    max_concurrent_tasks: int = 1
    cpu_cores: Optional[int] = 1  # Auto-detect if None
    memory_bytes: Optional[int] = (1800)  # Auto-detect if None (bytes)
    
    # Task execution settings
    task_timeout_seconds: int = 300  # 5 minutes
    python_executable: str = "python"
    
    # Data cache settings
    cache_size_limit_mb: float = 2048  # 2GB = 2048MB
    
    # Logging
    log_level: str = "INFO"
    
    # Directories
    temp_dir: str = os.path.join('.', 'temp', 'worker_data')
    cache_dir: str = os.path.join('.', 'worker_app', 'vm', 'shared')
    shared_dir: str = os.path.join('.', 'worker_app', 'vm', 'shared')

    username: str = "nourr"
    password: str = "123456"
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables."""
        return cls(
            worker_id=os.getenv('WORKER_ID', cls.worker_id),
            listen_port=int(os.getenv('WORKER_PORT', cls.listen_port)),
            master_address=os.getenv('MASTER_ADDRESS', cls.master_address),
            max_concurrent_tasks=int(os.getenv('MAX_CONCURRENT_TASKS', cls.max_concurrent_tasks)),
            task_timeout_seconds=int(os.getenv('TASK_TIMEOUT_SECONDS', cls.task_timeout_seconds)),
            log_level=os.getenv('LOG_LEVEL', cls.log_level),
            temp_dir=os.getenv('TEMP_DIR', cls.temp_dir),
            cache_dir=os.getenv('CACHE_DIR', cls.cache_dir),
            username=os.getenv('CLIENT_USERNAME', cls.username),
            password=os.getenv('CLIENT_PASSWORD', cls.password),
        )
    
    def ensure_directories(self):
        """Create necessary directories if they don't exist."""
        os.makedirs(self.temp_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)