@echo off
REM Script Batch para iniciar N Puntos de Recarga (CPs) secuenciales (CP1 a CPn)
REM El puerto de comunicación ENGINE-MONITOR comienza en 10001 y se incrementa.

REM --- CONFIGURACIÓN DE RED ---
SET ENGINE_HOST=localhost
SET ENGINE_PORT_BASE=10001
SET CENTRAL_ADDR=172.21.42.19:9090
SET KAFKA_BROKER=172.21.42.19:9092

REM *** RUTA CORREGIDA DE PYTHON (Usando la ubicación de AppData) ***
SET PYTHON_EXE=python 

REM Número de CPs a levantar (se toma del argumento, o 5 por defecto si no se da)
SET NUM_CPS=%1
IF "%NUM_CPS%"=="" SET NUM_CPS=5

ECHO.
ECHO --- Iniciando %NUM_CPS% Puntos de Recarga (CP1 a CP%NUM_CPS%) ---
ECHO --- Puertos Engine/Monitor: %ENGINE_PORT_BASE% al %CENTRAL_ADDR% ---
ECHO.

REM El bucle FOR /L va desde 1 hasta %NUM_CPS%
FOR /L %%I IN (1, 1, %NUM_CPS%) DO (
    REM Llamar a la subrutina para manejar la expansión de variables
    CALL :START_CP_PROCESS %%I
)

ECHO.
ECHO --- %NUM_CPS% CPs iniciados. Deberías ver las ventanas de consola. ---
GOTO :EOF


:START_CP_PROCESS
    REM %1 es el índice actual (1, 2, 3, ...)
    SET CP_ID=CP%1
    
    REM Calcular el puerto del Engine (10001, 10002, 10003, ...)
    SET /A ENGINE_PORT=%ENGINE_PORT_BASE% + %1 - 1
    
    REM Dirección completa que el MONITOR usará para conectarse al ENGINE
    SET ENGINE_ADDR=%ENGINE_HOST%:%ENGINE_PORT%
    
    ECHO --^> [%1/%NUM_CPS%] Iniciando CP: %CP_ID% en puerto %ENGINE_PORT%...
    
    REM ** 1. EJECUCIÓN DEL ENGINE **
    REM cmd /k ejecuta la siguiente cadena. Usamos %PYTHON_EXE% (que es 'python')
    START "Engine %CP_ID% (Port %ENGINE_PORT%)" cmd /k "%PYTHON_EXE% EV_CP_E.py %KAFKA_BROKER% %ENGINE_PORT%"
    
    REM Esperar un momento para asegurar que el Engine está escuchando
    TIMEOUT /T 1 /NOBREAK > NUL
    
    ECHO --^> Iniciando Monitor (%CP_ID%)...
    
    REM ** 2. EJECUCIÓN DEL MONITOR **
    START "Monitor %CP_ID%" cmd /k "%PYTHON_EXE% EV_CP_M.py %ENGINE_ADDR% %CENTRAL_ADDR% %CP_ID%"
    
    REM Pequeño retardo entre el inicio de cada par
    TIMEOUT /T 1 /NOBREAK > NUL
    GOTO :EOF