import os
from worker_app.vm.vm import VMTaskExecutor
import time
import subprocess

def _run_bat_as_admin_subprocess(cmd):
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
        

if __name__ == "__main__":
    cmd = r'"./worker_app/vm/smb.bat"'
    _run_bat_as_admin_subprocess(cmd)
    print('success')
    time.sleep(10)