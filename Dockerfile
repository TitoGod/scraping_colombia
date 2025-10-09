# Usa una imagen base de Python con soporte para Playwright
FROM mcr.microsoft.com/playwright/python:v1.42.0-jammy

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Copia el archivo de requerimientos y los instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instala los navegadores de Playwright
RUN playwright install --with-deps

# Copia el resto del código de la aplicación
COPY src/ src/

# Comando por defecto que se ejecutará al iniciar el contenedor
CMD ["python", "src/handler/sync_colombia_trademarks.py"]