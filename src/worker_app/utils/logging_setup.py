"""Logging configuration for worker node."""

import logging
import sys
from typing import Optional


def setup_logging(worker_id: Optional[str] = None, log_level: str = 'INFO') -> logging.Logger:
    """Set up logging configuration.
    
    Args:
        worker_id: Optional worker identifier
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger('worker')
    
    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(getattr(logging, log_level))
        
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, log_level))
        
        # Create formatter
        formatter = logging.Formatter(
            f'%(asctime)s - %(name)s - {worker_id or "worker"} - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(handler)
    
    return logger 