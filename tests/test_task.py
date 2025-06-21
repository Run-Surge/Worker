import grpc
import os
import sys
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'protos'))
from protos.worker_pb2 import TaskAssignment, OutputDataInfo, DataNotification
from protos.worker_pb2_grpc import WorkerServiceStub


def create_data_notification(task_id: int, data_id: int):
    return DataNotification(
        task_id=task_id,
        data_id=data_id,
        data_name="test.txt",
        ip_address="localhost",
        port=5000,
        hash="1234567890",
    )

def create_task_assignment():
    return TaskAssignment(
        task_id=12,
        python_file=b"""
print("Hello world!")
with open('test.txt', 'r') as f:
    print(f.read())

    """,
        python_file_name="test.py",
        required_data_ids=[
            1,
        ],
        job_id=1,
        output_data_infos=[
            OutputDataInfo(
                data_id=3,
                data_name="test.txt"
            )
        ]
    )

def create_node_stub(address: str):
    channel = grpc.insecure_channel(address)
    return WorkerServiceStub(channel)

def test_task_assignment():
    task_assignment = create_task_assignment()
    print(task_assignment)
    stub = create_node_stub('localhost:50051')
    response = stub.AssignTask(task_assignment)
    print(response)
    
    for data in task_assignment.required_data_ids:
        print(f"Notifying data {data}")        
        data_notification = create_data_notification(task_assignment.task_id, data)
        print(data_notification)
        response = stub.NotifyData(data_notification)
        print(response)
        time.sleep(10)
        

test_task_assignment()