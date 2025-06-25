import os
from worker_app.vm.vm import VMTaskExecutor
import time

if __name__ == "__main__":
    ## Testing Example
    ## shared_dir is one for the host, put the files on it task.py, data.txt
    ## run the script (vm.py)
    ## see the result in the result.txt
    shared_dir = os.path.join("worker_app", "vm", "shared")
    disk_image = os.path.join("worker_app", "vm", "RunSugre-VM.qcow2")
    
    vm_executor = VMTaskExecutor(
        shared_folder_host=os.path.abspath(shared_dir),
        disk_image=disk_image
    )
    vm_executor.vm_running = True
    vm_executor.establish_ssh_connection()
    pid = vm_executor.execute_command_in_background("python3 /root/30.py")
    print(f"30.py executed with PID: {pid}")
    while True:
        start_time = time.time()
        memory_usage, error = vm_executor.get_process_memory_usage(pid)
        if memory_usage:
            print(f"Memory usage: {memory_usage} bytes")
        else:
            print(f"Error: {error}")
            break
        end_time = time.time()
        print(f"Time taken: {end_time - start_time} seconds")
        time.sleep(1)
        
