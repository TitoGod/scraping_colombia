# Proyecto de Sincronización de Marcas - Colombia

Este proyecto es una aplicación robusta diseñada para automatizar la extracción, procesamiento y carga (ETL) de datos de marcas desde el portal SIPI de Colombia, manteniendo una base de datos PostgreSQL sincronizada.

## ✨ Características Principales

-   **Scraping Automatizado**: Extrae datos de marcas por Clases Niza y por rangos de fechas configurables.
-   **Proceso ETL Completo**: Normaliza los datos extraídos, los compara con la información existente en la base de datos y realiza inserciones o actualizaciones de forma inteligente.
-   **Verificación y Corrección**: Incluye un flujo para detectar registros activos en la base de datos que ya no se encuentran en la fuente, re-scrapearlos para obtener su estado actual y corregirlos.
-   **Arquitectura Profesional**: El código está organizado en una arquitectura de capas desacoplada (`gateways`, `functions`, `services`, `handlers`) para facilitar su mantenimiento, escalabilidad y testing.
-   **Ejecución por Línea de Comandos**: Utiliza `argparse` para una ejecución flexible y parametrizable, ideal para entornos de servidor como Lightsail.

---

## 🏗️ Arquitectura del Proyecto

El proyecto sigue un patrón de diseño inspirado en aplicaciones de contenedores, separando claramente las responsabilidades:

```
/
├── src/
│   ├── gateways/               # Capa de acceso a sistemas externos (BD, Web Scraper).
│   ├── functions/              # Módulos con la lógica de negocio y orquestación.
│   ├── handler/                # Punto de entrada del contenedor/script.
│   ├── service/                # Capa delgada que valida y delega al orquestador.
│   └── utils/                  # Utilidades reutilizables (logging, constantes).
│
├── trigger_local_execution.sh  # Script para facilitar la ejecución local.
├── requirements.txt            # Dependencias del proyecto.
└── .env                        # Archivo para variables de entorno (NO subir a Git).
```

-   **`handler`**: El punto de entrada principal. Su única función es recibir la petición y llamar al `service` correspondiente.
-   **`service`**: Valida los parámetros del evento y delega la ejecución a la función de orquestación principal.
-   **`functions`**: Contienen la lógica de negocio de alto nivel (ej: "ejecutar el scraping completo", "correr el proceso ETL").
-   **`gateways`**: Contienen la lógica de bajo nivel para interactuar directamente con sistemas externos (Playwright para la web, Psycopg2 para la base de datos).

---

## 🚀 Guía de Instalación y Configuración

Sigue estos pasos para poner en marcha el proyecto en tu entorno local o en un servidor.

### 1. Prerrequisitos

-   Python 3.10 o superior.
-   Acceso a una base de datos PostgreSQL.

### 2. Instalar Dependencias

```bash
# Instala todas las librerías necesarias
pip install -r requirements.txt

# Playwright requiere un paso adicional para instalar los navegadores que controla
playwright install
```

---

## ▶️ Cómo Ejecutar el Proyecto

El proyecto está diseñado para ser ejecutado desde la línea de comandos, pasando los parámetros necesarios.

### Ejecución Simplificada (Recomendado)

Para facilitar la ejecución local, puedes usar el script `trigger_local_execution.sh`. Este script configura los parámetros y ejecuta el handler por ti.

1.  **Dar permisos de ejecución al script (solo la primera vez):**
    ```bash
    chmod +x trigger_local_execution.sh
    ```

2.  **Ejecutar el proceso:**
    ```bash
    ./trigger_local_execution.sh
    ```
    Si deseas ejecutar el proceso para marcas inactivas, simplemente edita el script y cambia `"active"` por `"inactive"`.

### Ejecución Manual

También puedes ejecutar el script directamente y pasar los argumentos manualmente. Esto es especialmente útil en servidores o en scripts de automatización.

```bash
# Ejecutar para el estado "activo"
python src/handler/sync_colombia_trademarks.py --status active

# Ejecutar para el estado "inactivo"
python src/handler/sync_colombia_trademarks.py --status inactive
```

Todos los logs de la ejecución se guardarán en el archivo `etl_process_en.log` en la raíz del proyecto.