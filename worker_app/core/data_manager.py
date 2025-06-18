import os
import hashlib
import uuid
from protos import common_pb2

class DataManager:

    @staticmethod
    def _verify_hash(file_path: str, hash_value: str):

        return True
    
        #TODO enable this when we have a way to verify the hash
        with open(file_path, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()
            if file_hash != hash_value:
                return False
        return True
    
    @staticmethod
    def _recieve_data(request_iterator, data_info: common_pb2.DataInfo, shared_dir: str):
        temp_file_path = os.path.join(shared_dir, data_info.job_id, str(uuid.uuid4()))
        os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
        
        
        with open(temp_file_path, 'wb') as f:
            for request in request_iterator:
                if not request.HasField('chunk'):
                    raise ValueError("Invalid request")
                data_chunk = request.chunk
                f.write(data_chunk.chunk_data)
                if data_chunk.is_last_chunk:   
                    break 
        return temp_file_path


    @staticmethod
    def recieveData(request_iterator, data_info: common_pb2.DataInfo, shared_dir: str):
        file_path = os.path.join(shared_dir, data_info.job_id, data_info.file_name)
        if os.path.exists(file_path):
           raise Exception(f"File already exists")

        temp_file_path = DataManager._recieve_data(request_iterator, data_info, shared_dir)
        if not DataManager._verify_hash(temp_file_path, data_info.hash):
            return False, None
        
        os.rename(temp_file_path, file_path)
        return True, file_path

    @staticmethod
    def streamData(data_id: str):
        pass