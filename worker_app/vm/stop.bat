@echo off
set "VBOX_MANAGE=%~1"
if "%VBOX_MANAGE%"=="" set "VBOX_MANAGE=C:\Program Files\Oracle\VirtualBox\VBoxManage.exe"

:: Check if VM is running
"%VBOX_MANAGE%" showvminfo RunSurge | findstr "running" >nul
if %errorlevel% neq 0 (
    echo VM 'RunSurge' is not running 1>&2
    exit /b 1
)

echo Stopping VM
"%VBOX_MANAGE%" controlvm RunSurge acpipowerbutton
