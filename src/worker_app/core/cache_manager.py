"""Data caching implementation."""

import os
import shutil
import time
import hashlib
import pickle
import traceback
from dataclasses import dataclass
from typing import Dict, Any, Optional
from ..config import WorkerConfig
from ..utils.logging_setup import setup_logging

@dataclass
class CacheEntry:
    """Cache entry."""
    path: str
    size_bytes: int
    last_accessed: float


class CacheManager:
    """Manages local data caching for the worker node."""
    
    def __init__(self, config: WorkerConfig):
        """Initialize data cache with configuration."""
        self.config = config
        self.logger = setup_logging(config.worker_id, config.log_level)
        self.cache_dir = config.cache_dir
        self.size_limit_mb = config.cache_size_limit_mb
        
        # Ensure cache directory exists
        self.logger.info(f"Initializing cache manager with cache directory: {self.cache_dir}")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Initialize cache tracking
        self._initialize_cache()
        
        # Add a dummy item for testing
        self._add_dummy_cache_item()
    
    def _add_dummy_cache_item(self):
        """Add a dummy item to the cache for testing purposes."""
        try:
            # Create a dummy data structure
            
            
            # Create a unique ID for the dummy data
            dummy_id = 'test_data_001'
            
            # Create a temporary file to store the data
            temp_file = os.path.join(self.cache_dir, f"{dummy_id}.txt")
            
        
            self.logger.info(f"Dummy data path: {temp_file}")
            # Add to cache entries
            dummy_files = [
               'test_1mb.txt',
               'test_10mb.txt',
               'test_50mb.txt',
               'test_100mb.txt',
               'test_250mb.txt',
               'test_500mb.txt'
            ]
            for file in dummy_files:
                file_path = os.path.join(self.cache_dir, file)
                size_bytes = os.path.getsize(file_path) 
                self.cache_entries[file] = CacheEntry(
                    path=file_path,
                    size_bytes=size_bytes,
                    last_accessed=time.time()   
                    )
                # self.total_size_mb += size_bytes
            
            self.logger.info(f"Added dummy cache item with ID: {dummy_id}")
            
        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(f"Failed to add dummy cache item: {e}")
    
    def _initialize_cache(self):
        """Initialize cache tracking."""
        self.cache_entries: Dict[str, CacheEntry] = {}
        self.total_size_mb = 0
        
        # Load existing cache entries
        for entry in os.listdir(self.cache_dir):
            entry_path = os.path.join(self.cache_dir, entry)
            if os.path.isdir(entry_path):
                size_mb = self._get_dir_size_mb(entry_path)
                self.cache_entries[entry] = CacheEntry(
                    path=entry_path,
                    size_bytes=size_bytes,
                    last_accessed=time.time()
                )
                self.total_size_mb += size_mb
            elif entry.endswith('.pkl'):  # Handle pickle files
                size_bytes = os.path.getsize(entry_path)
                entry_id = os.path.splitext(entry)[0]
                self.cache_entries[entry_id] = CacheEntry(
                    path=entry_path,
                    size_bytes=size_bytes,
                    last_accessed=time.time()
                )
                self.total_size_mb += size_bytes
    
    def _get_dir_size_mb(self, path: str) -> float:
        """Get directory size in MB.
        
        Args:
            path: Directory path
            
        Returns:
            float: Directory size in MB
        """
        total_size = 0
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return total_size
    
    def _generate_cache_key(self, data_id: str) -> str:
        """Generate a cache key for data.
        
        Args:
            data_id: Data identifier
            
        Returns:
            str: Cache key
        """
        return hashlib.md5(data_id.encode()).hexdigest()
    
    def get_cache_entry(self, data_id: str) -> Optional[CacheEntry]:
        """Get data from cache.
        
        Args:
            data_id: Data identifier
            
        Returns:
            Optional[CacheEntry]: Cache entry or None if not found
        """
        # cache_key = self._generate_cache_key(data_id)
        cache_key = data_id
        if cache_key in self.cache_entries:
            # Update last accessed time
            self.cache_entries[cache_key].last_accessed = time.time()
            self.logger.info(f"Cache entry {cache_key} accessed at {time.time()}")
            self.logger.debug(f"Cache entry path: {self.cache_entries[cache_key].path}")
            return self.cache_entries[cache_key]
        
        return None
    

    def put(self, data_id: str, data_path: str) -> bool:
        """Put data into cache.
        
        Args:
            data_id: Data identifier
            data_path: Path to data to cache
            
        Returns:
            bool: True if data was cached successfully
        """
        cache_key = self._generate_cache_key(data_id)
        data_size_bytes = self._get_dir_size_mb(data_path)
        
        # Check if we need to make space
        if self.total_size_mb + data_size_bytes > self.size_limit_mb:
            if not self._make_space(data_size_bytes):
                self.logger.error("Failed to make space in cache")
                return False
        
        try:
            # Copy data to cache
            cache_path = os.path.join(self.cache_dir, cache_key)
            shutil.copytree(data_path, cache_path)
            
            # Update cache tracking
            self.cache_entries[cache_key] = CacheEntry(
                path=cache_path,
                size_bytes=data_size_bytes,
                last_accessed=time.time()
            )
            self.total_size_mb += data_size_bytes
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to cache data {data_id}: {e}")
            return False
    
    def _make_space(self, required_bytes: int) -> bool:
        """Make space in cache for new data.
        
        Args:
            required_bytes: Required space in bytes
            
        Returns:
            bool: True if space was made successfully
        """
        # Sort entries by last accessed time
        sorted_entries = sorted(
            self.cache_entries.items(),
            key=lambda x: x[1].last_accessed
        )
        
        # Remove oldest entries until we have enough space
        while self.total_size_mb + required_bytes > self.size_limit_mb and sorted_entries:
            key, entry = sorted_entries.pop(0)
            try:
                shutil.rmtree(entry.path)
                self.total_size_mb -= entry.size_bytes
                del self.cache_entries[key]
            except Exception as e:
                self.logger.error(f"Failed to remove cache entry {key}: {e}")
                return False
        
        return self.total_size_mb + required_bytes <= self.size_limit_mb
    
    def get_status(self) -> Dict[str, Any]:
        """Get cache status.
        
        Returns:
            Dict[str, Any]: Cache status information
        """
        return {
            'total_size_mb': self.total_size_mb,
            'size_limit_mb': self.size_limit_mb,
            'entries': len(self.cache_entries),
            'cache_dir': self.cache_dir
        }
    
    def cleanup(self):
        """Clean up cache directory."""
        try:
            shutil.rmtree(self.cache_dir)
            os.makedirs(self.cache_dir, exist_ok=True)
            self._initialize_cache()
        except Exception as e:
            self.logger.error(f"Failed to cleanup cache: {e}") 