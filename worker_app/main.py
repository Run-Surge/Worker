"""Main entry point for the worker node."""

import argparse
import signal
import sys
import time
from concurrent import futures
import grpc

from .config import Config
from .core.worker_manager import WorkerManager
from .grpc_services.worker_servicer import WorkerServicer
from .utils.logging_setup import setup_logging
# from .security.interceptor import AuthenticationServerInterceptor


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Distributed Computing Worker Node")
    
    parser.add_argument(
        '--worker-id', 
        type=str, 
        default=None,
        help='Unique identifier for this worker (default: auto-generated)'
    )
    
    parser.add_argument(
        '--port', 
        type=int, 
        default=50051,
        help='Port to listen on (default: 50051)'
    )
    
    parser.add_argument(
        '--master-address', 
        type=str, 
        default='localhost:50050',
        help='Address of the master node (default: localhost:50050)'
    )
    
    parser.add_argument(
        '--max-jobs', 
        type=int, 
        default=1,
        help='Maximum number of concurrent jobs (default: 1)'
    )
    
    parser.add_argument(
        '--log-level', 
        type=str, 
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--cpu-cores', 
        type=int, 
        default=None,
        help='Number of CPU cores to use (default: auto-detect)'
    )
    
    parser.add_argument(
        '--memory-mb', 
        type=int, 
        default=None,
        help='Amount of memory in MB to use (default: auto-detect)'
    )
    
    parser.add_argument(
        '--cache-size-mb', 
        type=int, 
        default=2048,
        help='Data cache size limit in MB (default: 2048)'
    )
    
    return parser.parse_args()


def create_config(args) -> Config:
    """Create worker configuration from command-line arguments and environment."""
    # Start with environment-based config
    config = Config.from_env()
    
    # Override with command-line arguments
    if args.worker_id:
        config.worker_id = args.worker_id
    
    config.listen_port = args.port
    config.master_address = args.master_address
    config.max_concurrent_jobs = args.max_jobs
    config.log_level = args.log_level
    config.cache_size_limit_mb = args.cache_size_mb
    
    if args.cpu_cores:
        config.cpu_cores = args.cpu_cores
    if args.memory_mb:
        config.memory_mb = args.memory_mb
    
    return config


def create_grpc_server(worker_servicer: WorkerServicer, port: int) -> grpc.Server:
    """Create and configure the gRPC server."""
    # Create server with thread pool
    #TODO: add authentication interceptor
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10),
                         interceptors=[])
    
    # Add servicer to server
    import worker_pb2_grpc
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(worker_servicer, server)
    
    # Bind to port
    listen_addr = f'[::]:{port}'
    server.add_insecure_port(listen_addr)
    
    return server


def setup_signal_handlers(server: grpc.Server, worker_manager: WorkerManager):
    """Set up signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, starting graceful shutdown...")
        
        # Stop accepting new requests
        server.stop(grace=30)  # 30 second grace period
        
        # Shutdown worker manager
        worker_manager.shutdown()
        
        print("Shutdown complete")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    """Main function to start the worker node."""
    try:
        # Parse command-line arguments
        args = parse_arguments()
        
        # Create configuration
        config = create_config(args)
        
        # Set up logging
        logger = setup_logging(config.worker_id, config.log_level)
        
        logger.info(f"Starting worker node {config.worker_id}")
        logger.info(f"Configuration: port={config.listen_port}, "
                   f"master={config.master_address}, "
                   f"max_jobs={config.max_concurrent_jobs}")
        
        # Create worker manager
        logger.info("Initializing worker manager...")
        worker_manager = WorkerManager(config)
        
        # Create gRPC servicer
        logger.info("Creating gRPC servicer...")
        worker_servicer = WorkerServicer(worker_manager, config)
        
        # Create and start gRPC server
        logger.info(f"Starting gRPC server on port {config.listen_port}...")
        server = create_grpc_server(worker_servicer, config.listen_port)
        
        # Set up signal handlers for graceful shutdown
        setup_signal_handlers(server, worker_manager)
        
        # Start the server
        server.start()
        

        #TODO: register the worker with the master

        logger.info(f"Worker {config.worker_id} is running and ready to accept jobs")
        logger.info(f"Listening on port {config.listen_port}")
        
        # Display resource information
        resource_status = worker_manager.resource_pool.get_resource_status()
        logger.info(f"Available resources: {resource_status['cpu_total']} CPU cores, "
                   f"{resource_status['memory_total_mb']:.1f}MB memory")
        
        try:
            # This enables the server to run indefinitely
            server.wait_for_termination()
                
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down...")
            
    except Exception as e:
        print(f"Failed to start worker: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main() 