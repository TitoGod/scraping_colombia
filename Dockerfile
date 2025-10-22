# 1. Usar una imagen base de Python que incluya Playwright
FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy

# 2. Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# 3. Copiar el archivo de requerimientos e instalarlos
# Esto se hace por separado para aprovechar el caché de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiar todo el código fuente del proyecto al contenedor
COPY . .

# 5. Definir el comando por defecto para ejecutar la aplicación
ENTRYPOINT ["python", "-m", "src.handler.sync_colombia_trademarks"]