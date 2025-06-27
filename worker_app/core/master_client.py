import os
import time
import grpc.aio
import asyncio
from ..config import Config
from protos import common_pb2
from protos import master_pb2
from protos import master_pb2_grpc
from protos import worker_pb2_grpc
from ..utils.logging_setup import setup_logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from ..utils.security import security_manager
import traceback
import zipfile

class MasterClient:
    def __init__(self, config: Config):
        print(f'master_address {config.master_address}')
        self.master_address = config.master_address
        self.shared_dir = config.shared_dir
        self.logger = setup_logging(config.log_level)


    @asynccontextmanager
    async def _get_master_stub(self) -> AsyncGenerator[master_pb2_grpc.MasterServiceStub, None]:
        channel = None
        try:
            channel = grpc.aio.insecure_channel(self.master_address, options=[
                ('grpc.enable_http_proxy', 0),
                ('grpc.so_reuseaddr', 1),
                ('grpc.use_local_subchannel_pool', 1),
                ('grpc.dns_resolver_option.use_ipv4_first', 1),
                ('grpc.dns_resolver_option.use_ipv6', 0)
            ])
            stub = master_pb2_grpc.MasterServiceStub(channel)
            yield stub
        except Exception as e:
            self.logger.error(f"Failed to create stub to master: {e}")
            raise
        finally:
            if channel:
                try:
                    await channel.close()
                except Exception as e:
                    self.logger.warning(f"Error closing channel: {e}")

    @asynccontextmanager
    async def _get_worker_stub(self, worker_address: str) -> AsyncGenerator[worker_pb2_grpc.WorkerServiceStub, None]:
        channel = None
        try:
            channel = grpc.aio.insecure_channel(worker_address)
            stub = worker_pb2_grpc.WorkerServiceStub(channel)
            yield stub
        except Exception as e:
            self.logger.error(f"Failed to create stub to worker: {e}")
            raise
        finally:
            if channel:
                try:
                    await channel.close()
                except Exception as e:
                    self.logger.warning(f"Error closing channel: {e}")

    async def _stream_data(self, data_metadata: common_pb2.DataMetadata, response_iterator: AsyncGenerator[common_pb2.DataChunk, None]):
        data_path = os.path.join(self.shared_dir, str(data_metadata.task_id), data_metadata.data_name)
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        
        if os.path.exists(data_path):
            raise Exception(f"File {data_path} already exists")
        
        temp_path = data_path + '.temp'
        with open(temp_path, 'wb') as f:
            async for chunk in response_iterator:
                f.write(chunk.chunk_data)
                if chunk.is_last_chunk:
                    break
    
        if data_metadata.is_zipped:
            self.logger.debug(f'unzipping {temp_path} to {data_path}')
            with zipfile.ZipFile(temp_path, 'r') as zip_ref:
                zip_ref.extractall(os.path.dirname(data_path))
        else:
            self.logger.debug(f'renaming {temp_path} to {data_path}')
            os.rename(temp_path, data_path)

        return data_path
            

    async def stream_data(self, data_metadata: common_pb2.DataMetadata) -> str:
        """
        Stream data from master or worker and return the path to the data
        """
        if data_metadata.is_on_master:
            self.logger.info(f"Streaming data {data_metadata.data_id} for task {data_metadata.task_id} from master")
            async with self._get_master_stub() as stub:
                try:
                    response_iterator = stub.StreamData(common_pb2.DataIdentifier(data_id=data_metadata.data_id))
                    path = await self._stream_data(data_metadata, response_iterator)
                    return path
                except Exception as e:
                    self.logger.error(f"Failed to get data from master: {e}")
                    raise
        else:   
            self.logger.info(f"Streaming data {data_metadata.data_id} for task {data_metadata.task_id} from worker {data_metadata.ip_address}:{data_metadata.port}")
            worker_address = f"{data_metadata.ip_address}:{data_metadata.port}"
            async with self._get_worker_stub(worker_address) as stub:
                try:
                    response_iterator = stub.StreamData(
                        common_pb2.DataIdentifier(
                            data_id=data_metadata.data_id)
                        )
                    path = await self._stream_data(data_metadata, response_iterator)
                    return path
                except Exception as e:
                    self.logger.error(f"Failed to get data from master: {e}")
                    raise


    async def register_worker(self, config: Config):
        async with self._get_master_stub() as stub:
            try:
                print(f'registering with port {config.listen_port}')
                print(f'registering with memory {config.memory_bytes}')
                response = await stub.NodeRegister(master_pb2.NodeRegisterRequest(
                    username=config.username,
                    password=config.password,
                    machine_fingerprint=security_manager.get_machine_fingerprint(),
                    memory_bytes=config.memory_bytes,
                    port=config.listen_port
                ))
                config.worker_id = response.node_id
                return response
            except Exception as e:
                # print(traceback.format_exc())
                self.logger.error(f"Failed to register worker: {e}")
                raise


    async def deregister_worker(self, config: Config):
        async with self._get_master_stub() as stub:
            try:
                response = await stub.NodeUnregister(master_pb2.NodeUnregisterRequest(
                    node_id=config.worker_id
                ))
                return response
            except Exception as e:
                self.logger.error(f"Failed to deregister worker: {e}")
                raise

    async def task_complete(self, task_id: int, average_memory_bytes: int, total_time_elapsed: float):
        async with self._get_master_stub() as stub:
            try:
                response = await stub.TaskComplete(master_pb2.TaskCompleteRequest(
                    task_id=task_id,
                    average_memory_bytes=average_memory_bytes,
                    total_time_elapsed=total_time_elapsed
                ))
                return response
            except Exception as e:
                self.logger.error(f"Failed to complete task: {e}")
                raise