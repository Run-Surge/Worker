import os
import sys
import grpc
import tempfile
import hashlib
import time
from pathlib import Path
import time
import traceback

# Add the protos directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'protos'))

from protos import common_pb2, worker_pb2, worker_pb2_grpc

def calculate_file_hash(file_path: str) -> str:
    """Calculate SHA256 hash of a file."""
    with open(file_path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()

def create_data_upload_iterator(file_path: str, data_id: str, task_id: str, file_name: str):
    """Create an iterator that yields DataUploadRequest messages for file upload."""
    # Calculate file hash
    try:
        file_hash = calculate_file_hash(file_path)
        file_size = os.path.getsize(file_path)
        
        # Create DataInfo message
        data_info = common_pb2.DataInfo(
            data_id=data_id,
            task_id=task_id,
            file_name=file_name,
            size=str(file_size),
            hash=file_hash
        )
        
        # Yield the data info first
        print('yielding data info')
        yield worker_pb2.DataUploadRequest(data_info=data_info)
        
        # Read file in chunks and yield data chunks
        chunk_size = 1024 * 1024  # 1MB chunks
        print('wowwwwwy')
        with open(file_path, 'rb') as f:
            while True:
                chunk_data = f.read(chunk_size)
                if not chunk_data:
                    break
                
                is_last_chunk = f.tell() >= file_size
                chunk = common_pb2.DataChunk(
                    chunk_data=chunk_data,
                    is_last_chunk=is_last_chunk
                )
                # Print chunk info
                print('yielding chunk')
                yield worker_pb2.DataUploadRequest(chunk=chunk)
    except Exception as e:
        print(traceback.format_exc())
        print(f"Error: {e}")

def test_receive_file():
    """Test the ReceiveData functionality using dummy_file.txt."""
    print("Starting RecieveFile test...")
    
    # Test configuration
    dummy_file_path = os.path.join(os.path.dirname(__file__), 'dummy_file.txt')
    data_id = 1
    task_id = 1
    file_name = "dummy_file.txt"
    
    # Verify dummy file exists
    if not os.path.exists(dummy_file_path):
        print(f"Error: Dummy file not found at {dummy_file_path}")
        return False
    
    print(f"Using dummy file: {dummy_file_path}")
    print(f"File size: {os.path.getsize(dummy_file_path)/1024/1024} MB")
    
    try:
        # Create gRPC channel and stub
        channel = grpc.insecure_channel('localhost:50051')
        stub = worker_pb2_grpc.WorkerServiceStub(channel)
        
        print("Connected to worker service")
        
        # Create data upload iterator
        request_iterator = create_data_upload_iterator(
            dummy_file_path, data_id, task_id, file_name
        )
        print(type(request_iterator))
        
        print(f"Uploading file with data_id: {data_id}, task_id: {task_id}")
        start_time = time.time()
        # for request in request_iterator:
        #     print(request)
        # Call ReceiveData
        response = stub.ReceiveData(request_iterator)
        
        end_time = time.time()
        upload_time = end_time - start_time
        
        print(f"Upload completed in {upload_time:.3f} seconds")
        print(f"Response success: {response.success}")
        print(f"Response message: {response.message}")
        
        # Verify the response
        assert response.success, f"Upload failed: {response.message}"
        print("✓ Upload successful")
        
        return True
        
    except grpc.RpcError as e:
        print(f"gRPC error: {e.code()}: {e.details()}")
        return False
    except Exception as e:
        print(f"Test error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if 'channel' in locals():
            channel.close()

if __name__ == "__main__":
    print("=" * 60)
    print("RecieveFile Test Suite")
    print("=" * 60)
    
    # Test 1: Basic file upload with dummy_file.txt
    success1 = test_receive_file()
    
    if success1:
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("❌ SOME TESTS FAILED")
        print("=" * 60)
        exit(1)
