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

class MasterClient:
    def __init__(self, config: Config):
        self.master_address = config.master_address
        self.shared_dir = config.shared_dir
        self.logger = setup_logging(config.log_level)


    @asynccontextmanager
    async def _get_master_stub(self) -> AsyncGenerator[master_pb2_grpc.MasterServiceStub, None]:
        try:
            channel = grpc.aio.insecure_channel(self.master_address)
            async with channel:
                stub = master_pb2_grpc.MasterServiceStub(channel)
                yield stub
        except Exception as e:
            self.logger.error(f"Failed to create stub to master: {e}")
            raise

    @asynccontextmanager
    async def _get_worker_stub(self, worker_address: str) -> AsyncGenerator[worker_pb2_grpc.WorkerServiceStub, None]:
        try:
            channel = grpc.aio.insecure_channel(worker_address)
            async with channel:
                stub = worker_pb2_grpc.WorkerServiceStub(channel)
                yield stub
        except Exception as e:
            self.logger.error(f"Failed to create stub to worker: {e}")
            raise

    async def _stream_data(self, data_metadata: common_pb2.DataMetadata, response_iterator: AsyncGenerator[common_pb2.DataChunk, None]):
        data_path = os.path.join(self.shared_dir, str(data_metadata.task_id), data_metadata.data_name)
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        
        if os.path.exists(data_path):
            raise Exception(f"File {data_path} already exists")

        with open(data_path, 'wb') as f:
            async for chunk in response_iterator:
                f.write(chunk.chunk_data)
                if chunk.is_last_chunk:
                    break

        return data_path
            

    async def stream_data(self, data_metadata: common_pb2.DataMetadata) -> str:
        """
        Stream data from master or worker and return the path to the data
        """
        if data_metadata.is_on_master:
            self.logger.info(f"Streaming data {data_metadata.data_id} for task {data_metadata.task_id} from master")
            async with self._get_master_stub() as stub:
                try:
                    response_iterator = stub.StreamData(data_id=data_metadata.data_id)
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
                            id=data_metadata.data_id)
                        )
                    path = await self._stream_data(data_metadata, response_iterator)
                    return path
                except Exception as e:
                    self.logger.error(f"Failed to get data from master: {e}")
                    raise