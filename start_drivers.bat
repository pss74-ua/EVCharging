@echo off
setlocal enabledelayedexpansion

:: ===========================================
:: SCRIPT: start_drivers.bat 
:: ===========================================

SET PYTHON_EXE=python


:: --- Configuracion Numerica y Strings ---
:: Cambiar IP a la de la Central
set KAFKA_BROKER=localhost:9092 
set REQUEST_FOLDER=requests_files
set /a MAX_REQUESTS=20
set DRIVER_SCRIPT=EV_Driver.py

:: --- Validacion de Argumentos ---
IF "%~1"=="" (
    echo Error: Debe especificar el numero de drivers a iniciar.
    echo Uso: %%~n0 ^<numero_de_drivers^>
    goto :eof
)

set NUM_DRIVERS=%1

echo.
echo ===========================================
echo  Iniciando %NUM_DRIVERS% EV Drivers...
echo  Broker: %KAFKA_BROKER%
echo ===========================================
echo.

:: --- Bucle de Inicio de Drivers ---
for /l %%i in (1, 1, %NUM_DRIVERS%) do (
    
    :: 1. Obtener el índice absoluto
    set /a ABS_INDEX=%%i

    :: 2. Llamar a la subrutina para calcular el índice cíclico
    call :CALC_INDEX !ABS_INDEX!
    
    set REQUEST_INDEX=!R_INDEX!
    
    :: 3. Definir ID y archivo
    set DRIVER_ID=DRIVER!REQUEST_INDEX!
    set REQUEST_FILE=!REQUEST_FOLDER!\requests!REQUEST_INDEX!.txt
    
    echo [DRIVER %%i] ID: !DRIVER_ID!, Archivo: !REQUEST_FILE!
    
    ::  4. EJECUCIÓN DEFINITIVA:
    :: - /D "%~dp0": Inicia la nueva ventana de CMD en el directorio del script.
    :: - cmd /k: Ejecuta y mantiene la ventana abierta para que veas el output Batch.
    START "%DRIVER_ID%" /D "%~dp0" cmd /k "%PYTHON_EXE% !DRIVER_SCRIPT! %KAFKA_BROKER% !DRIVER_ID! !REQUEST_FILE!"
    
    :: Pausa minima
    timeout /t 0 /nobreak >nul
)

echo.
echo ===========================================
echo  Drivers iniciados. Revise las ventanas.
echo ===========================================

goto :eof

:: ===========================================
:: SUBRUTINA DE CALCULO DE ÍNDICE CÍCLICO
:: ===========================================
:CALC_INDEX
    set /a R_INDEX=%1
    set /a MAX_REQUESTS_LOC=!MAX_REQUESTS!
    
    :cycle_check_loc
    if !R_INDEX! GTR !MAX_REQUESTS_LOC! (
        set /a R_INDEX=!R_INDEX! - !MAX_REQUESTS_LOC!
        goto cycle_check_loc
    )
    
    if !R_INDEX! EQU 0 (
        set /a R_INDEX=!MAX_REQUESTS_LOC!
    )
    
    exit /b