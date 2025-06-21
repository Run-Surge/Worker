import os
import grpc
import sys
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'protos'))
from protos import common_pb2, worker_pb2_grpc

def get_array_size(array):
    size = 0
    for item in array:
        size += len(item)
    return size

def test_stream_data(data_id):
    # Create a gRPC channel
    channel = grpc.insecure_channel('localhost:5000')
    stub = worker_pb2_grpc.WorkerServiceStub(channel)

    # Request the dummy data
     
    request = common_pb2.DataIdentifier(id=data_id)

    # First check if data exists and get metadata
    # metadata = stub.GetDataMetadata(request)
    # assert metadata.found == True

    # Create temp directory to store streamed data
    temp_dir = './temp2'
    os.makedirs(temp_dir, exist_ok=True)
    print(os.path.abspath(temp_dir))
    output_file = os.path.join(temp_dir, f"{data_id}.txt")
    start_time = time.time()
    # Stream the data and write to file
    chunks = []

    for chunk in stub.StreamData(request):
        chunks.append(chunk.chunk_data)
        if chunk.is_last_chunk:
            break
    
    print(f"Size of chunks: {get_array_size(chunks) / 1024 / 1024} MB")

    # with open(output_file, 'wb') as f:
    #     for chunk in chunks:
    #         f.write(chunk)

    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")
    # Verify file exists and has content
    # assert os.path.exists(output_file)
    # assert os.path.getsize(output_file) > 0

    # Cleanup
    # os.remove(output_file)
    # os.rmdir(temp_dir)

files = [
    1,
    # 'test_10mb.txt',
    # 'test_50mb.txt',
    # 'test_100mb.txt',
    # 'test_250mb.txt',
    # 'test_500mb.txt'
]

for file in files:
    test_stream_data(file)