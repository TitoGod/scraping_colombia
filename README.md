# Sync Colombia Trademarks (Pipeline de ETL)

Este proyecto es un pipeline de ETL robusto diseñado para extraer, procesar y sincronizar datos de marcas registradas desde el portal SIPI (Superintendencia de Industria y Comercio) de Colombia.

Utiliza scraping web para obtener los datos, los normaliza y los carga en una base de datos PostgreSQL, asegurando que la información local esté actualizada.

## 🚀 Características Principales

  * **Scraping Paralelo:** Ejecuta tareas de scraping en paralelo usando `multiprocessing` para maximizar la eficiencia (un proceso para Clases Niza e históricos, otro para datos recientes).
  * **ETL por Lotes:** Procesa los archivos JSON generados en lotes (uno por uno) para realizar la carga en la base de datos, evitando sobrecargas de memoria.
  * **Auto-Corrección:** Incluye un flujo de verificación que identifica registros activos en la BD que faltan en los JSON (por fallos de scraping), los re-extrae individualmente por su número de solicitud y actualiza sus estados.
  * **Reportes en S3:** Genera un reporte de cambios (`change_report.csv`) en cada ejecución y un reporte de registros faltantes (`missing_records.csv`) durante la corrección, y los sube automáticamente a un bucket de S3.
  * **Monitoreo de Errores:** Integrado con Rollbar para el monitoreo de excepciones y mensajes de estado en tiempo real.
  * **Simulación Humana:** Utiliza un `user_agent` y `viewport` específicos para simular un navegador real (Chrome en Windows) y evitar bloqueos por parte del sitio de SIPI.

## ⚙️ Flujo del Proceso

El pipeline se ejecuta en el siguiente orden:

1.  **Inicio:** El proceso se invoca desde `src/handler/sync_colombia_trademarks.py`, que recibe el argumento `--status` (`active` o `inactive`).
2.  **Scraping Paralelo (`sync_orchestrator.py`):** Se lanzan dos procesos:
      * **Worker 1:** Extrae datos por Clases Niza (1-45) y datos históricos (1900-2018).
      * **Worker 2:** Extrae datos recientes (2019-Presente) con una granularidad más fina (semanal y diaria).
      * Todos los resultados se guardan como archivos JSON en la carpeta temporal `tmp/`.
3.  **ETL Principal (`etl_functions.py`):**
      * Una vez que *ambos* workers de scraping terminan, el proceso principal lee los archivos JSON de `tmp/` en lotes.
      * Normaliza los datos (formatea fechas, estados, titulares).
      * Compara cada lote con la base de datos PostgreSQL, identificando registros nuevos y modificados.
      * Realiza `INSERT` para registros nuevos y `UPDATE` para registros existentes que cambiaron.
      * Genera y sube el `change_report.csv` a S3.
4.  **Verificación y Corrección (`etl_functions.py`):**
      * Compara todos los `request_number` de la BD (con estado activo) contra todos los `request_number` encontrados en los JSON.
      * Si encuentra registros en la BD que no están en los JSON, genera `missing_records.csv` y lo sube a S3.
      * Inicia un scraping secundario (`run_scraping_for_missing_requests`) que visita el sitio de SIPI y busca cada registro faltante por su número.
      * Guarda los resultados de la corrección en un nuevo JSON y actualiza los estados en la BD.
5.  **Limpieza:** Al finalizar todo el proceso, la carpeta `tmp/` y su contenido son eliminados.

## 🛠️ Tecnologías Utilizadas

  * Python
  * Playwright (para scraping web asíncrono)
  * Pandas (para manipulación de datos y generación de CSV)
  * Psycopg2 (para conectividad con PostgreSQL)
  * Boto3 (para conectividad con AWS S3)
  * Rollbar (para monitoreo de errores)
  * Multiprocessing (para paralelismo)

## 🔧 Configuración

### 1\. Prerrequisitos

  * Python 3.8+
  * Una base de datos PostgreSQL accesible.
  * Credenciales de AWS (con permisos de escritura en S3).
  * Un token de acceso de Rollbar.

### 2\. Instalación

1.  Clona este repositorio:
    ```bash
    git clone [URL_DEL_REPOSITORIO]
    cd [NOMBRE_DEL_REPOSITORIO]
    ```
2.  (Recomendado) Crea y activa un entorno virtual:
    ```bash
    python -m venv venv
    source venv/bin/activate  # En Windows: venv\Scripts\activate
    ```
3.  Instala las dependencias:
    ```bash
    pip install -r requirements.txt
    ```
4.  Instala los navegadores necesarios para Playwright:
    ```bash
    playwright install
    ```

### 3\. Variables de Entorno

Crea un archivo `.env` en la raíz del proyecto. Este archivo es **crítico** para la configuración de la aplicación.

```ini
# --- Base de Datos (PostgreSQL) ---
PG_USER="tu_usuario_db"
PG_PASS="tu_contraseña_db"
PG_HOST="tu_host_db"
PG_PORT="5432"
PG_DB="tu_nombre_db"
TABLE="nombre_de_la_tabla_marcas"

# --- Monitoreo (Rollbar) ---
ROLLBAR_TOKEN="tu_token_de_rollbar"
ENV_STAGE="development" # o "production"

# --- AWS (S3) ---
AWS_ACCESS_KEY_ID="tu_access_key_id"
AWS_SECRET_ACCESS_KEY="tu_secret_access_key"
AWS_REGION="tu_region_aws" # ej: "us-east-1"
```

## 🏃 Cómo Ejecutar

El script principal es `src/handler/sync_colombia_trademarks.py`. Debe ejecutarse pasando el argumento `--status` para definir qué tipo de marcas se van a scrapear.

**Para sincronizar marcas activas:**

```bash
python src/handler/sync_colombia_trademarks.py --status active
```

**Para sincronizar marcas inactivas:**

```bash
python src/handler/sync_colombia_trademarks.py --status inactive
```

## 📁 Estructura del Proyecto

```
.
├── src/
│   ├── functions/        # Lógica de negocio principal
│   │   ├── etl_functions.py
│   │   ├── scraping_functions.py
│   │   └── sync_orchestrator.py
│   ├── gateways/         # Módulos para interactuar con servicios externos
│   │   ├── database_gateway.py
│   │   ├── s3_gateway.py
│   │   └── scraping_gateway.py
│   ├── handler/          # Punto de entrada de la aplicación
│   │   └── sync_colombia_trademarks.py
│   ├── middlewares/      # Configuraciones de middlewares
│   │   └── rollbar_config.py
│   ├── services/         # Lógica del handler
│   │   └── sync_colombia_trademarks/
│   │       └── main.py
│   └── utils/            # Utilidades, constantes y normalización
│       ├── constants.py
│       ├── data_normalizer.py
│       └── logging_config.py
├── .env                  # (Tú debes crearlo)
├── etl_process_en.log    # (Generado en ejecución)
└── requirements.txt      # (Asegúrate de tenerlo)
```