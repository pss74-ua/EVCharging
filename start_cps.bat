@echo off
REM Script Batch actualizado para la Release 2 (Entorno Virtual + Registry)
SET ENGINE_HOST=localhost
SET ENGINE_PORT_BASE=10001
SET CENTRAL_ADDR=localhost:9090
SET KAFKA_BROKER=localhost:9092
SET REGISTRY_ADDR=localhost:6000

SET PYTHON_EXE=python 
SET VENV_ACTIVATE=entvirtual\Scripts\activate

SET NUM_CPS=%1
IF "%NUM_CPS%"=="" SET NUM_CPS=5

ECHO --- Iniciando %NUM_CPS% Puntos de Recarga (CP1 a CP%NUM_CPS%) ---

FOR /L %%I IN (1, 1, %NUM_CPS%) DO (
    CALL :START_CP_PROCESS %%I
)
GOTO :EOF

:START_CP_PROCESS
    SET CP_ID=CP%1
    SET /A ENGINE_PORT=%ENGINE_PORT_BASE% + %1 - 1
    SET ENGINE_ADDR=%ENGINE_HOST%:%ENGINE_PORT%
    
    ECHO --^> Iniciando CP: %CP_ID% (Puerto Engine: %ENGINE_PORT%)
    
    REM ** EJECUCIÓN ENGINE **
    REM Se activa el entorno y luego se lanza el Engine
    START "Engine %CP_ID%" cmd /k "call %VENV_ACTIVATE% && %PYTHON_EXE% EV_CP_E.py %KAFKA_BROKER% %ENGINE_PORT%"
    
    TIMEOUT /T 1 /NOBREAK > NUL
    
    REM ** EJECUCIÓN MONITOR **
    REM Se añade el parámetro REGISTRY_ADDR requerido en la Release 2
    START "Monitor %CP_ID%" cmd /k "call %VENV_ACTIVATE% && %PYTHON_EXE% EV_CP_M.py %ENGINE_ADDR% %CENTRAL_ADDR% %REGISTRY_ADDR% %CP_ID%"
    
    TIMEOUT /T 1 /NOBREAK > NUL
    GOTO :EOF