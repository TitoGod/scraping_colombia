import logging
import sys
import os
import rollbar

def setup_logging():
    """
    Configura el sistema de logging para registrar en un archivo y en la consola,
    e inicializa Rollbar si está configurado.
    """
    logger = logging.getLogger("etl_app")
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        return logger

    # Configuración de Rollbar
    rollbar_access_token = os.getenv('ROLLBAR_ACCESS_TOKEN')
    if rollbar_access_token:
        rollbar.init(
            access_token=rollbar_access_token,
            environment='production',  # O el entorno que corresponda
            code_version='1.0.0'
        )
        logger.info("Rollbar successfully configured.")
    else:
        logger.warning("ROLLBAR_ACCESS_TOKEN not found. Rollbar is not configured.")

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler('etl_process_en.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger