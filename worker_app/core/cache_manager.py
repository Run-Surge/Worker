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
from ..utils.util import create_data_path

@dataclass
class CacheEntry:
    """Cache entry."""
    path: str
    size_bytes: int
    last_accessed: float = time.time()


class CacheManager:
    """Manages local data caching for the worker node."""
    
    def __init__(self, config: Config):
        """Initialize data cache with configuration."""
        self.config = config
        self.logger = setup_logging(config.worker_id, config.log_level)
        self.cache_dir = config.cache_dir
        # Ensure cache directory exists
        self.logger.info(f"Initializing cache manager with cache directory: {self.cache_dir}")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Initialize cache tracking
        self._initialize_cache()
        
    def _add_dummy_data(self):
        path  = create_data_path(self.config.shared_dir, 1, 'test.txt')
        path2  = create_data_path(self.config.shared_dir, 1, 'test2.txt')
        self.cache_entries = {
            1 : CacheEntry(path=path, size_bytes=os.path.getsize(path)),
            2 : CacheEntry(path=path2, size_bytes=os.path.getsize(path)),
        }

    def _initialize_cache(self):
        """Initialize cache tracking."""
        self.cache_entries: Dict[int, CacheEntry] = {}
        # self._add_dummy_data()
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
    
    def get_cache_entry(self, data_id: int) -> CacheEntry:
        return self.cache_entries.get(data_id, None)

    def add_cache_entry(self, data_id: int, cache_entry: CacheEntry) -> bool:
        self.logger.debug(f"Adding data {data_id} to cache")
        if data_id in self.cache_entries:
            self.logger.warning(f"Data {data_id} already in cache")
            return False
        
        #TODO: check if file path exists

        self.cache_entries[data_id] = cache_entry
        return True


