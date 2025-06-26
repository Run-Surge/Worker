import paramiko
import subprocess
import sys
import time
import os
import logging
import threading
import platform
from typing import Optional, Tuple, Dict, Any
import socket
import psutil
import ctypes
import traceback

class VMTaskExecutor:    
    def __init__(self, 
                 disk_image: str = "./RunSugre-VM.qcow2",
                 ssh_username: str = "root", 
                 ssh_password: str = "1234",
                 ssh_port: int = 2222,
                 memory_bytes: int = 1024 * 1024 * 1024,
                 cpus: int = 2,
                 vm_startup_timeout: int = 90,
                 task_timeout: int = 120,
                 shared_folder_host: str = "",
                 shared_folder_guest: str = "/mnt/win",
                 smb_username: str = "qemuguest",
                 smb_password: str = "1234",
                 smb_share_name: str = "qemu_share",
                 log_level: str = "INFO"):
        """
        Initialize the VM Task Executor.
        
        Args:
            disk_image: Path to the QEMU disk image
            ssh_username: SSH username for VM connection
            ssh_password: SSH password for VM connection
            ssh_port: SSH port for VM connection
            memory: VM memory in MB
            cpus: Number of CPU cores for VM
            vm_startup_timeout: Timeout for VM startup in seconds
            task_timeout: Timeout for task execution in seconds
            shared_folder_host: Local directory path to share with VM
            shared_folder_guest: Mount point in VM for shared folder
            smb_username: SMB username for Windows file sharing
            smb_password: SMB password for Windows file sharing
            smb_share_name: SMB share name
            log_level: Logging level
        """
        self.disk_image = disk_image
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_port = ssh_port
        self.memory_bytes = memory_bytes
        self.cpus = cpus
        self.vm_startup_timeout = vm_startup_timeout
        self.task_timeout = task_timeout
        self.shared_folder_host = shared_folder_host
        self.shared_folder_guest = shared_folder_guest
        self.smb_username = smb_username
        self.smb_password = smb_password
        self.smb_share_name = smb_share_name
        self.vm_process: Optional[subprocess.Popen] = None
        self.vm_running = False
        
        self.ssh_client: Optional[paramiko.SSHClient] = None
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        # Suppress paramiko's verbose logging
        logging.getLogger("paramiko").setLevel(logging.CRITICAL)
        logging.getLogger("paramiko.transport").setLevel(logging.CRITICAL)
        logging.getLogger("paramiko.transport.sftp").setLevel(logging.WARNING)
    
    def _get_qemu_executable(self) -> str:
            possible_paths = [
                # should add possible paths here 
                "C:\\Program Files\\qemu\\qemu-system-x86_64.exe",
            ]
            
            for path in possible_paths:
                if os.path.exists(path) or (path == "qemu-system-x86_64.exe" and shutil.which(path)):
                    self.logger.info(f"Found QEMU at: {path}")
                    return path
    def _load_config_from_env(self, vm_config_file: str = "vm_config.env", 
                             ssh_config_file: str = "ssh_config.env"):
        """Load configuration from environment files."""
        try:
            if os.path.exists(vm_config_file):
                with open(vm_config_file, 'r') as f:
                    for line in f:
                        if line.strip() and not line.startswith('#'):
                            key, value = line.strip().split('=', 1)
                            if key == "DEFAULT_DISK_IMAGE":
                                self.disk_image = value
                            elif key == "AVAILABLE_PORTS":
                                ports = [int(p.strip()) for p in value.split(',')]
                                self.ssh_port = ports[0]
            
            if os.path.exists(ssh_config_file):
                with open(ssh_config_file, 'r') as f:
                    for line in f:
                        if line.strip() and not line.startswith('#'):
                            key, value = line.strip().split('=', 1)
                            if key == "SSH_USERNAME":
                                self.ssh_username = value
                            elif key == "SSH_PASSWORD":
                                self.ssh_password = value
                            elif key == "SHARED_FOLDER_HOST":
                                self.shared_folder_host = value
                            elif key == "SHARED_FOLDER_GUEST":
                                self.shared_folder_guest = value
                            elif key == "TASK_TIMEOUT":
                                self.task_timeout = int(value)
                            elif key == "VM_STARTUP_TIMEOUT":
                                self.vm_startup_timeout = int(value)
                                
        except Exception as e:
            self.logger.warning(f"Failed to load config from files: {e}")
    
    def _check_admin_privileges(self) -> bool:
        try:
            return ctypes.windll.shell32.IsUserAnAdmin()
        except Exception as e:
            self.logger.warning(f"Could not check administrator privileges: {e}")
            return False
        
    def _run_bat_as_admin_subprocess(self, cmd):
        """Alternative method using subprocess with PowerShell elevation"""
        try:
            print(f"Running {cmd} with admin privileges via PowerShell")
            # Use PowerShell to run the batch file with elevation
            powershell_cmd = f'Start-Process -FilePath "{cmd}" -Verb RunAs -Wait'
            result = subprocess.run(
                ["powershell", "-Command", powershell_cmd],
                capture_output=True,
                text=True,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
            if result.returncode == 0:
                print(f"Successfully started {cmd} with admin privileges via PowerShell")
                return True
            else:
                print(f"Failed to start {cmd}. Error: {result.stderr}")
                return False
        except Exception as e:
            print(f"Error running bat file via PowerShell: {e}")
            return False

    def _setup_smb_with_batch(self) -> bool:
        try:
            self.logger.info("Setting up SMB share using smb.bat...")
            smb_script = os.path.join(os.path.dirname(__file__), "smb.bat")
            print(f"SMB Script: {smb_script}")
            if not os.path.exists(smb_script):
                self.logger.error("smb.bat not found in the current directory")
                return False
            print(f"SMB Username: {self.smb_username}")
            print(f"SMB Password: {self.smb_password}")
            print(f"Shared Folder: {self.shared_folder_host}")
            print(f"SMB Share Name: {self.smb_share_name}")
            cmd = f'{smb_script} "{self.smb_username}" "{self.smb_password}" "{self.smb_share_name}" "{self.shared_folder_host}"'            
            print(cmd)
            self._run_bat_as_admin_subprocess(cmd)
            return True        
            
        except Exception as e:
            self.logger.error(f"Failed to setup SMB share: {e}")
            return False
    def _launch_vm_directly(self) -> bool:
        """
        Launch VM directly as a subprocess of Python.
        """
        try:
            self.logger.info("Launching VM directly...")
            qemu_executable = self._get_qemu_executable()
            memory_mib = int(self.memory_bytes / (1024 * 1024)) # QEMU -m expects MiB

            if not os.path.exists(qemu_executable):
                self.logger.error(f"QEMU executable not found at: {qemu_executable}")
                return False
            if not os.path.exists(self.disk_image):
                self.logger.error(f"Disk image not found at: {self.disk_image}")
                return False

            cmd_list = [
                qemu_executable,
                "-m", str(memory_mib),
                "-smp", str(self.cpus),
                "-cpu", "qemu64",
                "-accel", "tcg",
                "-net", "nic",
                "-net", f"user,hostfwd=tcp::{self.ssh_port}-:22",
                "-hda", os.path.abspath(self.disk_image),
                "-display", "none",
                "-nographic",
            ]

            creation_flags = 0
            if os.name == 'nt': # If running on Windows
                # CREATE_NEW_PROCESS_GROUP makes the child process the root of a new process group.
                # When the parent dies, the OS can clean up this group more effectively.
                # It often works in conjunction with job objects for robust cleanup.
                creation_flags |= subprocess.CREATE_NEW_PROCESS_GROUP
                creation_flags |= subprocess.CREATE_NO_WINDOW # To prevent console window pop-up

            print(cmd_list)
            self.vm_process = subprocess.Popen(
                cmd_list,
                # Use shell=True for Windows compatibility like the batch method
                shell=False,
                creationflags=creation_flags,
                # Don't capture QEMU output - let it run freely
                stdout=None,
                stderr=None
            )
            self.logger.info(f"VM process (PID: {self.vm_process.pid}) started directly.")
            return True

        except Exception as e:
            self.logger.error(f"Failed to launch VM directly: {e}")
            return False
    def _launch_vm_with_batch(self) -> bool:
        """
        Launch VM using startup.bat script with parameters.
        """
        try:
            self.logger.info("Launching VM using startup.bat...")
            startup_script = os.path.join(os.path.dirname(__file__), "startup.bat")
            if not os.path.exists(startup_script):
                self.logger.error("startup.bat not found in the current directory")
                return False
            
            cmd = f'"{startup_script}" "{self.memory_bytes / 1024 / 1024}" "{self.cpus}" "{self.ssh_port}" "{self.disk_image}" "{self._get_qemu_executable()}"'          
            try:
                self.vm_process = subprocess.Popen(
                    cmd,
                    shell=True,
                    text=True,
                )
                self.logger.info("VM process started via startup.bat")
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to execute startup.bat: {e}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to launch VM with batch file: {e}")
            return False
            
    
    def launch_vm(self) -> bool:
        if self.vm_running:
            self.logger.warning("VM is already running")
            return True
        try:
            start_time = time.time()
            self.logger.info("Starting VM launch process...")

            if not self.shared_folder_host:
                self.logger.warning("No shared folder configured")
                return False
            
            if not os.path.exists(self.shared_folder_host):
                os.makedirs(self.shared_folder_host)
            
            if self._setup_smb_with_batch():
                self.logger.info(f"SMB share configured: {self.shared_folder_host} -> {self.shared_folder_guest}")
            else:
                self.logger.error("Failed to setup SMB share")
                return False
            
            if not self._launch_vm_directly():
                self.logger.error("Failed to launch VM using startup.bat")
                return False
            
            self.logger.info("VM process started, waiting for SSH connection...")
            if self._wait_for_ssh_connection():
                self.vm_running = True
                if self.shared_folder_host:
                    self._setup_shared_folder()
                elapsed_time = time.time() - start_time
                self.logger.info(f"VM launched successfully in {elapsed_time:.2f} seconds")
                return True
            else:
                elapsed_time = time.time() - start_time
                self.logger.error(f"VM failed to start or SSH connection failed after {elapsed_time:.2f} seconds")
                self.stop_vm()
                return False
                
        except Exception as e:
            elapsed_time = time.time() - start_time
            self.logger.error(f"Failed to launch VM after {elapsed_time:.2f} seconds: {e}")
            return False
    
    def _wait_for_ssh_connection(self) -> bool:
        """Wait for SSH connection to become available."""
        start_time = time.time()
        while time.time() - start_time < self.vm_startup_timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex(("localhost", self.ssh_port))
                sock.close()
                
                if result == 0:
                    if self._establish_ssh_connection():
                        return True
                        
            except Exception:
                pass
            
            time.sleep(2)
        
        return False
    
    def _establish_ssh_connection(self) -> bool:
        try:
            if self.ssh_client:
                self.ssh_client.close()
            
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                "localhost", 
                port=self.ssh_port, 
                username=self.ssh_username, 
                password=self.ssh_password,
                timeout=10
            )
            self.logger.info("SSH connection established")
            return True
            
        except Exception as e:
            self.logger.debug(f"SSH connection attempt failed: {e}")
            return False
    
    def _setup_shared_folder(self) -> bool:
        try:
            self.logger.info("Setting up shared folder in VM...")
            
            mkdir_cmd = f"mkdir -p {self.shared_folder_guest}"
            output, error = self._execute_command(mkdir_cmd)
            if error:
                self.logger.error(f"mkdir error: {error}")    
                return False
            check_mount_cmd = f"mount | grep {self.shared_folder_guest}"
            output, error = self._execute_command(check_mount_cmd)
            if output and self.shared_folder_guest in output:
                self.logger.info("Shared folder already mounted")
                return True
            
            mount_cmd = f"mount -t cifs //10.0.2.2/{self.smb_share_name} {self.shared_folder_guest} -o user={self.smb_username},password={self.smb_password},vers=2.0"
            output, error = self._execute_command(mount_cmd)
            if error and "already mounted" not in error.lower():
                self.logger.error(f"Failed to mount SMB share: {error}")
                return False
            verify_cmd = f"ls -la {self.shared_folder_guest}"
            output, error = self._execute_command(verify_cmd)
            if error:
                self.logger.error(f"Failed to verify shared folder mount: {error}")
                return False
            
            self.logger.info(f"SMB shared folder mounted successfully at {self.shared_folder_guest}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup shared folder: {e}")
            return False
    
    
    def stop_vm(self) -> bool:
        try:
            if self.ssh_client:
                self.ssh_client.close()
                self.ssh_client = None
            
            if self.vm_process:
                self.vm_process.terminate()
                self.vm_process.kill()
                self.vm_process.wait()
                try:
                    self.vm_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.vm_process.kill()
                    self.vm_process.wait()
                
                self.vm_process = None
            
            self.vm_running = False
            self.logger.info("VM stopped successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop VM: {e}")
            return False
    
    def get_vm_status(self) -> Dict[str, Any]:
        status = {
            "vm_running": self.vm_running,
            "vm_process_alive": False,
            "ssh_connected": False,
            "ssh_port": self.ssh_port,
            "memory": self.memory_bytes,
            "cpus": self.cpus,
            "disk_image": self.disk_image,
            "shared_folder_host": self.shared_folder_host,
            "shared_folder_guest": self.shared_folder_guest,
            "shared_folder_mounted": False
        }
        
        if self.vm_process:
            status["vm_process_alive"] = self.vm_process.poll() is None
        
        if self.ssh_client:
            try:
                self.ssh_client.exec_command("echo test", timeout=5)
                status["ssh_connected"] = True
                
                if self.shared_folder_host and self.shared_folder_guest:
                    try:
                        stdin, stdout, stderr = self.ssh_client.exec_command(f"mount | grep {self.shared_folder_guest}", timeout=5)
                        mount_output = stdout.read().decode('utf-8', errors='ignore')
                        status["shared_folder_mounted"] = bool(mount_output.strip())
                    except Exception:
                        status["shared_folder_mounted"] = False
                        
            except Exception:
                status["ssh_connected"] = False
        
        return status
    
    def execute_command_in_background(self, command: str, cd_to_dir: Optional[str] = None, timeout: Optional[int] = None) -> Optional[int]:  
        """
        Execute a command in the background and return the PID of the process.
        The command must be a valid shell command.
        The command will be executed in the background.
        The command will return the PID of the process.
        """
        self.establish_ssh_connection()
        
        try:
            timeout = timeout or self.task_timeout

            command = f'nohup {command} 1> log.out 2> error.err & echo $!'
            if cd_to_dir:
                command = f'cd {cd_to_dir} && {command}'
                
            self.logger.debug(f"Executing command in background: {command}")

            stdin, stdout, stderr = self.ssh_client.exec_command(command, timeout=timeout)
            pid = stdout.read().decode('utf-8', errors='ignore')
            error = stderr.read().decode('utf-8', errors='ignore')
            if error:
                self.logger.error(f"Error executing command: {error}")
                return None
            
            self.logger.info(f"Command '{command}' executed in background with PID: {pid}") 
            return int(pid)
        except Exception as e:
            self.logger.error(f"Failed to execute command '{command}': {e}")
            return None
        
    def get_process_memory_usage(self, pid: int, timeout: Optional[int] = None) -> Optional[int]:
        """
        Get the memory usage of a process by PID.

        Returns:
            tuple: (memory_usage in bytes, error)
        """
        self.establish_ssh_connection()
        try:
            command = f"cat /proc/{pid}/cmdline | tr '\\0' ' '; cat /proc/{pid}/status | grep -E \"VmSize\""
            print(f"command is {command}")
            stdin, stdout, stderr = self.ssh_client.exec_command(command, timeout=timeout)
            output = stdout.read().decode('utf-8', errors='ignore')
            error = stderr.read().decode('utf-8', errors='ignore')
            
            if error:
                self.logger.error(f"Failed to get process memory usage for PID {pid}: {error}")
                return None, error
            
            if "python3" not in output:
                return None, "Process is not a Python process"
            print(f"output is {output}")
            memory_usage = int(output.split()[-2]) * 1024
            return memory_usage, None
        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(f"Failed to get process memory usage for PID {pid}: {e}")
            return None, str(e)
    
    def _execute_command(self, command: str, timeout: Optional[int] = None) -> Tuple[Optional[str], Optional[str]]:
        if not self.vm_running or not self.ssh_client:
            return None, "VM is not running or SSH not connected"
        
        try:
            timeout = timeout or self.task_timeout
            ## async command
            stdin, stdout, stderr = self.ssh_client.exec_command(command, timeout=timeout)
            ## sync command
            output = stdout.read().decode('utf-8', errors='ignore')
            error = stderr.read().decode('utf-8', errors='ignore')
            
            return output, error if error else None
            
        except Exception as e:
            self.logger.error(f"Failed to execute command '{command}': {e}")
            return None, str(e)

    def establish_ssh_connection(self) -> Optional[paramiko.SSHClient]:
        if self.ssh_client and self.ssh_client.get_transport().is_active():
            return self.ssh_client

        self.logger.info("Establishing SSH connection...")
        if not self._establish_ssh_connection():
            self.logger.error("Failed to establish SSH connection")
            raise Exception("Failed to establish SSH connection")
        
        self.logger.info("Setting up shared folder...")
        if not self._setup_shared_folder():
            self.logger.error("Failed to setup shared folder")
            raise Exception("Failed to setup shared folder")
        
        return self.ssh_client


    def execute_script(self, script_path: str, script_args: Optional[str] = None) -> Optional[int]:
        """
        Execute a Python script on the virtual machine using python3.
        Script must be accessible from within the VM (e.g., in shared folder).
        
        Args:
            script_path: Path to the Python script (VM path, e.g., /mnt/win/script.py)
            script_args: Arguments to pass to the script
            
        Returns:
            int/None: PID of the process or None if failed
        """
        try:
            start_time = time.time()
            script_name = os.path.basename(script_path)

            command = f"python3 {script_path}"
            
            if script_args:
                command += f" {script_args}"
            
            self.logger.info(f"Executing Python script: {script_name}")
            return self.execute_command_in_background(command, os.path.dirname(script_path))
        
        except Exception as e:
            elapsed_time = time.time() - start_time
            self.logger.error(f"Failed to execute script after {elapsed_time:.2f} seconds: {e}")
            return None
    
    def get_shared_folder_path(self, filename: str = "") -> str:
        if filename:
            return os.path.join(self.shared_folder_guest, filename).replace('\\', '/')
        return self.shared_folder_guest
    
    def execute_script_from_shared(self, script_filename: str, input_filename: str = "", output_filename: str = "") -> Tuple[Optional[str], Optional[str]]:
        if not self.shared_folder_host:
            return None, "No shared folder configured"
        
        input_path = self.get_shared_folder_path(input_filename)
        output_path = self.get_shared_folder_path(output_filename)        
        script_path = self.get_shared_folder_path(script_filename)
        script_args = f'"{input_path}" "{output_path}"'
        check_cmd = f"test -f {script_path}"
        _, error = self._execute_command(check_cmd)
        if error:
            return None, f"Script not found in shared folder: {script_filename}"
        
        return self.execute_script(script_path, script_args)
    
    def __enter__(self):
        """Context manager entry."""
        self._load_config_from_env()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_vm()

if __name__ == "__main__":
    ## Testing Example
    ## shared_dir is one for the host, put the files on it task.py, data.txt
    ## run the script (vm.py)
    ## see the result in the result.txt
    shared_dir = "."  
    
    vm_executor = VMTaskExecutor(shared_folder_host=shared_dir)
    if vm_executor.launch_vm():
        status = vm_executor.get_vm_status()
        vm_executor.logger.info(f"VM Status - Running: {status['vm_running']}, SSH Connected: {status['ssh_connected']}")
        
        if status.get('shared_folder_mounted'):
            vm_executor.logger.info(f"Shared folder mounted: {status['shared_folder_host']} -> {status['shared_folder_guest']}")
            script_output, script_error = vm_executor.execute_script_from_shared("task.py", "data.txt", "result.txt")
            if script_output:
                print("Script Console Output:")
                print("="*50)
                print(script_output)
                print("="*50)
            elif script_error:
                if "not found" in script_error:
                    vm_executor.logger.warning("No task.py found in shared folder. Please place your script in the shared directory.")
                    vm_executor.logger.info(f"Shared folder location: {shared_dir}")
                else:
                    vm_executor.logger.error(f"Script Error: {script_error}")
            
            result_file_path = os.path.join(shared_dir, "result.txt")
            if os.path.exists(result_file_path):
                vm_executor.logger.info("Result file found in shared folder")
                try:
                    with open(result_file_path, 'r') as f:
                        result_content = f.read()
                    print("\n" + "="*50)
                    print("RESULT FILE CONTENTS:")
                    print("="*50)
                    print(result_content)
                    print("="*50)
                except Exception as e:
                    vm_executor.logger.error(f"Failed to read result file: {e}")
            else:
                vm_executor.logger.info("No result file found in shared folder")
                
        else:
            vm_executor.logger.error("Shared folder not mounted. Cannot proceed without shared folder.")
            vm_executor.logger.info("Please ensure:")
            vm_executor.logger.info(f"1. Shared folder path exists: {shared_dir}")
            vm_executor.logger.info("2. VM has proper CIFS/SMB support")
            vm_executor.logger.info("3. VM has necessary mount permissions")
    vm_executor.stop_vm()
