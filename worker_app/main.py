"""Main entry point for the worker node."""

import os
import argparse
import asyncio
import logging
import threading
import signal
import sys
import time
import traceback
from concurrent import futures
import grpc.aio
from .config import Config
from .core.worker_manager import WorkerManager
from .grpc_services.worker_servicer import WorkerServicer
from .utils.logging_setup import setup_logging
from .core.master_client import MasterClient
from .utils.constants import SHUTDOWN_EVENT_NAME
import win32event
import win32api
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
        default=None,
        help='Port to listen on (default: 50051)'
    )
    
    parser.add_argument(
        '--master-address', 
        type=str, 
        default=None,
        help='Address of the master node (default: localhost:12345)'
    )
    
    parser.add_argument(
        '--max-tasks', 
        type=int, 
        default=1,
        help='Maximum number of concurrent tasks (default: 1)'
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
        '--memory-bytes', 
        type=int, 
        default=None,
        help='Amount of memory in bytes to use (default: auto-detect)'
    )
    
    parser.add_argument(
        '--cache-size-mb', 
        type=int, 
        default=2048,
        help='Data cache size limit in MB (default: 2048)'
    )
    
    parser.add_argument(
        '--username', 
        type=str, 
        default=None,
        help='Username for the worker (default: None)'
    )
    
    parser.add_argument(
        '--password', 
        type=str, 
        default=None,
        help='Password for the worker (default: None)'
    )

    return parser.parse_args()


def create_config(args) -> Config:
    """Create worker configuration from command-line arguments and environment."""
    # Start with environment-based config
    config = Config.from_env()
    
    # Override with command-line arguments
    if args.worker_id:
        config.worker_id = args.worker_id
    if args.port:
        config.listen_port = args.port
    if args.master_address:
        config.master_address = args.master_address
        config.master_ip_address, config.listen_port = args.master_address.split(':')
    config.max_concurrent_tasks = args.max_tasks
    config.log_level = args.log_level
    config.cache_size_limit_mb = args.cache_size_mb
    if args.username:
        config.username = args.username
    if args.password:
        config.password = args.password
    if args.cpu_cores:
        config.cpu_cores = args.cpu_cores
    if args.memory_bytes:
        config.memory_bytes = args.memory_bytes
    print(f'memory_bytes {config.memory_bytes}')
    
    return config


async def create_grpc_server(worker_servicer: WorkerServicer, port: int) -> grpc.Server:
    """Create and configure the gRPC server."""
    # Create server with thread pool
    #TODO: add authentication interceptor
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10),
                         interceptors=[])
    
    # Add servicer to server
    import worker_pb2_grpc
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(worker_servicer, server)
    
    # Bind to port
    listen_addr = f'[::]:{port}'
    server.add_insecure_port(listen_addr)
    
    return server

def deregister_from_master(config: Config):
    """Deregister the worker from the master."""
    try:
        master_client = MasterClient(config)
        asyncio.run(master_client.deregister_worker(config))
        print("Successfully deregistered from master")
    except Exception as e:
        print(f"Failed to deregister from master: {e}")

def _setup_windows_handler():
    security_attributes = None  # Use default security
    manual_reset = True
    initial_state = False
    try:
        print(f"Creating windows event {SHUTDOWN_EVENT_NAME}")
        shutdown_event = win32event.CreateEvent(
            security_attributes,
            manual_reset,
            initial_state,
            SHUTDOWN_EVENT_NAME
        )
    except Exception as e:
        print(f"Error creating event: {e}")
        # This might happen if the event already exists with different permissions
        # For this example, we'll just exit.
        return None

    return shutdown_event

def setup_signal_handlers(server: grpc.Server, worker_manager: WorkerManager, config: Config):
    """Set up signal handlers for graceful shutdown."""
    print("Setting up signal handlers")
    running_loop = asyncio.get_running_loop()
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, starting graceful shutdown...")
        
        # This is a hack to stop wait for the server to stop, without using await because handler shouldn't be async
        asyncio.run_coroutine_threadsafe(server.stop(grace=1), running_loop)  # 30 second grace period
        thread = threading.Thread(target=deregister_from_master, args=(config,))
        thread.start()
        thread.join()
        # Shutdown worker manager
        worker_manager.shutdown()
        
        print("Shutdown complete")
        os._exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    # signal.signal(signal.CTRL_C_EVENT, signal_handler)


async def register_with_master(config: Config):
    """Register the worker with the master."""
    try:    
        print(f'registering with master')
        master_client = MasterClient(config)
        response = await master_client.register_worker(config)
        if not response.success:
            raise Exception(response.message)
        config.worker_id = response.node_id
    except Exception as e:
        # print(f"Failed to register with master: {e}")
        print(f"Closing worker...")
        os._exit(1)

def wait_for_shutdown(shutdown_event):
    print("[Executor Thread] Now blocking and waiting for the event...")
    win32event.WaitForSingleObject(shutdown_event, win32event.INFINITE)
    print("[Executor Thread] Event was signaled!")
            
            
async def shutdown_listener(shutdown_event, loop, server: grpc.Server, worker_manager: WorkerManager, config: Config):
    """An asyncio-friendly coroutine that waits for the shutdown signal."""
    await loop.run_in_executor(
        None,  # Use the default thread pool executor
        wait_for_shutdown,
        shutdown_event
    )
    
    print("\nShutdown signal received by asyncio loop!")
    print(f"\nReceived windows signal, starting graceful shutdown...")
        
        # This is a hack to stop wait for the server to stop, without using await because handler shouldn't be async
    asyncio.run_coroutine_threadsafe(server.stop(grace=1), loop)  # 30 second grace period
    thread = threading.Thread(target=deregister_from_master, args=(config,))
    thread.start()
    thread.join()
    # Shutdown worker manager
    worker_manager.shutdown()
    await asyncio.sleep(2)
    print("Shutdown complete")
    os._exit(0)


async def main():
    """Main function to start the worker node."""
    try:
        # Parse command-line arguments
        args = parse_arguments()
        
        # Create configuration
        config = create_config(args)
        
        # Set up logging
        

        # Set up logging
        logger = setup_logging("main", config.log_level)
        
        logger.info(f"Starting worker node {config.worker_id}")
        logger.info(f"Configuration: port={config.listen_port}, "
                   f"master={config.master_address}, "
                   f"max_tasks={config.max_concurrent_tasks}")
        
        # Create worker manager
        logger.info("Initializing worker manager...")
        worker_manager = WorkerManager(config)
        
        # Create gRPC servicer
        logger.info("Creating gRPC servicer...")
        worker_servicer = WorkerServicer(worker_manager, config)
        #TODO: add authentication interceptor
        # Create and start gRPC server
        logger.info(f"Starting gRPC server on port {config.listen_port}...")
        server = await create_grpc_server(worker_servicer, config.listen_port)
        
        # Set up signal handlers for graceful shutdown
        setup_signal_handlers(server, worker_manager, config)
        shutdown_event = _setup_windows_handler()
        loop = asyncio.get_running_loop()
        loop.create_task(shutdown_listener(shutdown_event, loop, server, worker_manager, config))
        # Start the server
        await server.start()
        
        await register_with_master(config)

        logger.info(f"Worker {config.worker_id} is running and ready to accept tasks")
        logger.info(f"Listening on port {config.listen_port}")
        
        # Display resource information
        resource_status = worker_manager.resource_pool.get_resource_status()
        logger.info(f"Available resources: {resource_status['cpu_total']} CPU cores, "
                   f"{resource_status['memory_total_mb']:.1f}MB memory")
        
        try:
            # This enables the server to run indefinitely
            await server.wait_for_termination()
                
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down...")
            await deregister_from_master(config)
    except Exception as e:
        print(traceback.format_exc())
        print(f"Failed to start worker: {e}")
        sys.exit(1)