"""Logging configuration for worker node."""

import logging
import sys
from typing import Optional


def setup_logging(service_name: str = "Service_Name", log_level: str = 'INFO') -> logging.Logger:
    """Set up logging configuration.
    
    Args:
        worker_id: Optional worker identifier
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(service_name)
    
    # Set the log level (this can be updated on subsequent calls)
    logger.setLevel(getattr(logging, log_level))
    
    # Only configure handlers if not already configured
    if not logger.handlers:
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, log_level))
        
        # Create formatter
        formatter = logging.Formatter(
            f'%(asctime)s - {service_name} - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(handler)
        
        # Prevent propagation to root logger to avoid duplicate messages
        logger.propagate = False
    
    return logger 