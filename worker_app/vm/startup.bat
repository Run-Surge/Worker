@echo off
set "MEMORY=%~1"
set "VBOX_MANAGE=%~2"

if "%MEMORY%"=="" set "MEMORY=2048"
if "%VBOX_MANAGE%"=="" set "VBOX_MANAGE=C:\Program Files\Oracle\VirtualBox\VBoxManage.exe"

:: Check if VM is already running
"%VBOX_MANAGE%" showvminfo RunSurge | findstr "running" >nul 2>&1
if %errorlevel% equ 0 (
    echo VM 'RunSurge' is already running 1>&2
    exit /b 1
)

:: Allocate resources (only run once)
"%VBOX_MANAGE%" modifyvm RunSurge --memory %MEMORY%
"%VBOX_MANAGE%" startvm RunSurge --type headless
