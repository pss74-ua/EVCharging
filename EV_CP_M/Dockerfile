# Usa una imagen base ligera de Python
FROM python:3.10-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos de requerimientos e instálalos
# (Necesitarás instalar 'kafka-python' y posiblemente una librería de sockets o concurrencia)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el código fuente de la carpeta local al contenedor
COPY . .

# Comando de inicio de la aplicación (será sobrescrito por docker-compose.yml)
CMD ["python3", "EV_CP_M.py"]