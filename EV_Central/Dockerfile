# Usa una imagen base ligera de Python
# FROM python:3.10-slim

# Establece el directorio de trabajo dentro del contenedor
# WORKDIR /app

# Copia los archivos de requerimientos e instálalos
# (Necesitarás instalar 'kafka-python' y posiblemente una librería de sockets o concurrencia)
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el código fuente de la carpeta local al contenedor
# COPY . .

# Comando de inicio de la aplicación (será sobrescrito por docker-compose.yml)
# CMD ["python3", "EV_Central.py"]

# Usa una imagen base ligera de Python
FROM python:3.10-slim

# Instalar dependencias del sistema necesarias para confluent-kafka
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        librdkafka-dev \
        python3-dev \
        build-essential && \
    rm -rf /var/lib/apt/lists/*

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos de requerimientos e instálalos
COPY requirements.txt .
# Ahora 'pip install' funcionará porque las dependencias del sistema están presentes
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código fuente
COPY . .

# Comando de inicio (será usado por docker-compose)
CMD ["/bin/bash"]