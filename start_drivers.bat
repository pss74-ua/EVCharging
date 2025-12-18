@echo off
setlocal enabledelayedexpansion

SET KAFKA_BROKER=localhost:9092 
SET REQUEST_FOLDER=requests_files
SET /a MAX_REQUESTS=20
SET DRIVER_SCRIPT=EV_Driver.py
SET PYTHON_EXE=python
SET VENV_ACTIVATE=entvirtual\Scripts\activate

IF "%~1"=="" (
    echo Error: Debe especificar el numero de drivers.
    goto :eof
)

set NUM_DRIVERS=%1

echo ===========================================
echo  Iniciando %NUM_DRIVERS% EV Drivers con Entorno Virtual...
echo ===========================================

for /l %%i in (1, 1, %NUM_DRIVERS%) do (
    set /a ABS_INDEX=%%i
    call :CALC_INDEX !ABS_INDEX!
    set REQUEST_INDEX=!R_INDEX!
    
    set DRIVER_ID=DRIVER!REQUEST_INDEX!
    set REQUEST_FILE=!REQUEST_FOLDER!\requests!REQUEST_INDEX!.txt
    
    echo [DRIVER %%i] Lanzando !DRIVER_ID!...
    
    REM Se activa el entorno virtual en la nueva ventana antes de ejecutar el driver
    START "!DRIVER_ID!" /D "%~dp0" cmd /k "call %VENV_ACTIVATE% && %PYTHON_EXE% !DRIVER_SCRIPT! %KAFKA_BROKER% !DRIVER_ID! !REQUEST_FILE!"
    
    timeout /t 1 /nobreak >nul
)
goto :eof

:CALC_INDEX
    set /a R_INDEX=%1
    set /a MAX_REQUESTS_LOC=!MAX_REQUESTS!
:cycle_check_loc
    if !R_INDEX! GTR !MAX_REQUESTS_LOC! (
        set /a R_INDEX=!R_INDEX! - !MAX_REQUESTS_LOC!
        goto cycle_check_loc
    )
    if !R_INDEX! EQU 0 ( set /a R_INDEX=!MAX_REQUESTS_LOC! )
    exit /b