"""Data caching implementation."""

import os
import shutil
import time
import hashlib
import pickle
import traceback
from dataclasses import dataclass
from typing import Dict, Any, Optional
from ..config import Config
from ..utils.logging_setup import setup_logging

@dataclass
class CacheEntry:
    """Cache entry."""
    path: str
    size_bytes: int
    last_accessed: float


class CacheManager:
    """Manages local data caching for the worker node."""
    
    def __init__(self, config: Config):
        """Initialize data cache with configuration."""
        self.config = config
        self.logger = setup_logging(config.worker_id, config.log_level)
        self.cache_dir = config.cache_dir
        self.size_limit_bytes = config.cache_size_limit_mb * 1024 * 1024
        
        # Ensure cache directory exists
        self.logger.info(f"Initializing cache manager with cache directory: {self.cache_dir}")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Initialize cache tracking
        self._initialize_cache()
        
    def _initialize_cache(self):
        """Initialize cache tracking."""
        self.cache_entries: Dict[str, CacheEntry] = {}
        self.total_size_bytes = 0
        
        os.makedirs(self.cache_dir, exist_ok=True)
        if len(os.listdir(self.cache_dir)) == 0:
            return
        
        self.logger.info(f"Clearing cache directory: {self.cache_dir}")
        #TODO: fix cache initialization
        # for file in os.listdir(self.cache_dir):
            # os.remove(os.path.join(self.cache_dir, file))
        
    
    def _get_dir_size_bytes(self, path: str) -> int:
        """Get directory size in bytes.
        
        Args:
            path: Directory path
            
        Returns:
            int: Directory size in bytes
        """
        total_size = 0
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return total_size
    

    