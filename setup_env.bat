@echo off
echo --- Automatizando entorno para EVCharging (Python 3.13) ---

:: 1. Crear el entorno virtual forzando la version 3.13
echo [1/3] Creando entorno virtual 'entvirtual'...
py -3.13 -m venv entvirtual

:: 2. Activar para instalar librerias
echo [2/3] Activando entorno e instalando librerias...
call entvirtual\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

echo.
echo [3/3] LISTO! Entorno creado correctamente.
echo Para usarlo, ejecuta: entvirtual\Scripts\activate
pause