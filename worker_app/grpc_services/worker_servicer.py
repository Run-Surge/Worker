"""gRPC servicer implementation for the WorkerService."""

import sys
import os
import traceback
import time
from typing import Generator

import grpc
from google.protobuf.empty_pb2 import Empty

# Import generated protobuf modules
from protos import worker_pb2
from protos import worker_pb2_grpc
from protos import common_pb2

from protos.worker_pb2 import TaskAssignment, DataUploadRequest

from ..core.worker_manager import WorkerManager, WorkerState
from ..utils.logging_setup import setup_logging
from ..utils.util import format_bytes
from ..core.data_manager import DataManager
from ..config import Config

class WorkerServicer(worker_pb2_grpc.WorkerServiceServicer):
    """
    gRPC servicer implementation for WorkerService.
    
    Implements all RPC methods defined in worker.proto and delegates
    business logic to the WorkerManager instance.
    """
    
    def __init__(self, worker_manager: WorkerManager, config: Config):
        self.worker_manager = worker_manager
        self.worker_id = worker_manager.worker_id
        self.logger = setup_logging(self.worker_id)
        self.config = config
        self.logger.info(f"WorkerServicer initialized for worker {self.worker_id}")
    
    def GetWorkerStatus(self, request: Empty, context: grpc.ServicerContext) -> worker_pb2.WorkerStatus:
        """
        Get the current status of the worker.
        
        Args:
            request: Empty request
            context: gRPC context
            
        Returns:
            WorkerStatus proto with current worker state and task information
        """
        try:
            self.logger.debug("GetWorkerStatus called")
            
            # Get status from worker manager
            status_info = self.worker_manager.get_worker_status()
            
            # Map WorkerState enum to proto enum
            state_mapping = {
                WorkerState.UNSPECIFIED: worker_pb2.WORKER_STATE_UNSPECIFIED,
                WorkerState.IDLE: worker_pb2.WORKER_STATE_IDLE,
                WorkerState.BUSY: worker_pb2.WORKER_STATE_BUSY,
                WorkerState.INITIALIZING: worker_pb2.WORKER_STATE_INITIALIZING,
                WorkerState.SHUTTING_DOWN: worker_pb2.WORKER_STATE_SHUTTING_DOWN,
            }
            
            # Create and return WorkerStatus proto
            worker_status = worker_pb2.WorkerStatus(
                state=state_mapping.get(self.worker_manager.state, worker_pb2.WORKER_STATE_UNSPECIFIED),
                current_task_id=status_info.get("current_task_id", "")
            )
            
            self.logger.debug(f"Returning worker status: state={self.worker_manager.state.name}, "
                            f"current_task={status_info.get('current_task_id', 'none')}")
            
            return worker_status
            
        except Exception as e:
            self.logger.error(f"Error in GetWorkerStatus: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return worker_pb2.WorkerStatus()
    
    def AssignTask(self, request: TaskAssignment, context: grpc.ServicerContext) -> common_pb2.StatusResponse:
        """
        Assign a new task to the worker.
        
        Args:
            request: TaskAssignment proto with task details
            context: gRPC context
            
        Returns:
            StatusResponse indicating success or failure
        """
        try:
            task_id = request.task_id
            self.logger.info(f"AssignTask called for task {task_id}")
            
            # Delegate to worker manager
            success, message = self.worker_manager.assign_task(request)
            
            # Create status response
            response = common_pb2.StatusResponse(
                success=success,
                message=message
            )
            
            if success:
                self.logger.info(f"Task {task_id} assigned successfully")
            else:
                self.logger.warning(f"Task {task_id} assignment failed: {message}")
                # Set gRPC error code for task rejection
                if "insufficient" in message.lower() or "limit" in message.lower():
                    context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
                else:
                    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            
            return response
            
        except Exception as e:
            self.logger.error(f"Error in AssignTask: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return common_pb2.StatusResponse(
                success=False,
                message=f"Internal error: {str(e)}"
            )
    
    def GetDataMetadata(self, request: common_pb2.DataIdentifier, context: grpc.ServicerContext) -> common_pb2.DataMetadata:
        """
        Get metadata about cached data.
        
        Args:
            request: DataIdentifier with data ID
            context: gRPC context
            
        Returns:
            DataMetadata with information about the data
        """
        try:
            data_id = request.id
            self.logger.debug(f"GetDataMetadata called for data {data_id}")
            
            # Check if data exists in cache
            cached_data_entry = self.worker_manager.data_cache.get_cache_entry(data_id)
            
            if cached_data_entry is not None:
                # Data found in cache
                # Estimate size and type
                data_size = cached_data_entry.size_bytes
                
                # Determine data type (simplified for now)
                data_type = common_pb2.DATA_TYPE_PYTHON_PICKLE  # Default assumption
                
                response = common_pb2.DataMetadata(
                    found=True,
                    data_type=data_type,
                    total_size_bytes=data_size,
                    error_message=""
                )
                
                self.logger.debug(f"Data {data_id} found in cache, size: {data_size} bytes")
                
            else:
                # Data not found
                response = common_pb2.DataMetadata(
                    found=False,
                    data_type=common_pb2.DATA_TYPE_UNSPECIFIED,
                    total_size_bytes=0,
                    error_message=f"Data {data_id} not found in cache"
                )
                
                self.logger.debug(f"Data {data_id} not found in cache")
            
            return response
            
        except Exception as e:
            self.logger.error(f"Error in GetDataMetadata: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return common_pb2.DataMetadata(
                found=False,
                data_type=common_pb2.DATA_TYPE_UNSPECIFIED,
                total_size_bytes=0,
                error_message=f"Internal error: {str(e)}"
            )
    
    def StreamData(self, request: common_pb2.DataIdentifier, context: grpc.ServicerContext):
        """
        Stream cached data in chunks.
        
        Args:
            request: DataIdentifier with data ID
            context: gRPC context
            
        Yields:
            DataChunk proto messages with data chunks
        """
        try:
            data_id = request.id
            self.logger.debug(f"StreamData called for data {data_id}")
            
            # Get data from cache
            cached_data = self.worker_manager.data_cache.get_cache_entry(data_id)
            
            if cached_data is None:
                self.logger.warning(f"Data {data_id} not found for streaming")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Data {data_id} not found")
                return
            
            
            # Serialize data (for simplicity, using pickle)
            # import pickle
            
            
            # Stream data in chunks
            chunk_size = 1 * 1024 * 1024  # 1MB chunks
            total_size = int(cached_data.size_bytes)
            bytes_sent = 0
            
            self.logger.debug(f"Streaming data {data_id}, total size: {format_bytes(total_size)}")
            start_time = time.time()
            with open(cached_data.path, 'rb') as f:

                while bytes_sent < total_size:
                    # Calculate chunk boundaries
                    start = int(bytes_sent)
                    end = int(min(start + chunk_size, total_size))
                    chunk_data = f.read(chunk_size)
                    
                    # Create chunk
                    is_last = (end >= total_size)
                    chunk = common_pb2.DataChunk(
                        chunk_data=chunk_data,
                        is_last_chunk=is_last
                    )
                    
                    yield chunk
                    
                    bytes_sent = end
                    # self.logger.debug(f"Sent chunk {start}-{end} of {format_bytes(total_size)} (last: {is_last})")
            end_time = time.time()
            self.logger.debug(f"Completed streaming data {data_id} in {end_time - start_time} seconds")
            
        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(f"Error in StreamData: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
    
    def DeleteCachedData(self, request: common_pb2.DataIdentifier, context: grpc.ServicerContext) -> common_pb2.StatusResponse:
        """
        Delete data from the worker's cache.
        
        Args:
            request: DataIdentifier with data ID to delete
            context: gRPC context
            
        Returns:
            StatusResponse indicating success or failure
        """
        try:
            data_id = request.id
            self.logger.debug(f"DeleteCachedData called for data {data_id}")
            
            # Attempt to remove from cache
            removed = self.worker_manager.data_cache.remove(data_id)
            
            if removed:
                response = common_pb2.StatusResponse(
                    success=True,
                    message=f"Data {data_id} deleted from cache"
                )
                self.logger.debug(f"Successfully deleted data {data_id} from cache")
            else:
                response = common_pb2.StatusResponse(
                    success=False,
                    message=f"Data {data_id} not found in cache"
                )
                self.logger.debug(f"Data {data_id} not found for deletion")
            
            return response
            
        except Exception as e:
            self.logger.error(f"Error in DeleteCachedData: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return common_pb2.StatusResponse(
                success=False,
                message=f"Internal error: {str(e)}"
            ) 
        

    def ReceiveData(self, request_iterator: Generator[DataUploadRequest, None, None], context: grpc.ServicerContext) -> common_pb2.StatusResponse:
        """
        Upload data to the worker.
        
        Args:
            request: DataUploadRequest with data info and chunk
                - first one is data_info
                - rest are chunks
            context: gRPC context
            
        Returns:
            StatusResponse indicating success or failure
        """
        try:
            self.logger.debug(f"ReceiveData called")

            data_info = next(request_iterator).data_info
            self.logger.debug(f"UploadData called for data {data_info.data_id}")
            
            # Delegate to worker manager
            success, file_path = DataManager.recieveData(request_iterator, data_info, self.config.shared_dir)
            #TODO: maybe notify the task or something.

            # Create status response
            response = common_pb2.StatusResponse(
                success=success,
                message='Successfully received data' if success else 'Failed to receive data'
            )
            
            return response
            
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error in UploadData: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return common_pb2.StatusResponse(
                success=False,
                message=f"Internal error: {str(e)}"
            )