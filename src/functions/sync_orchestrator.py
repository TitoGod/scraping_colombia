import asyncio
import pathlib
import shutil
import os
import rollbar
from dotenv import load_dotenv
from src.functions.scraping_functions import (
    run_niza_class_scraping_concurrent,
    run_scraping_historical_part, 
    run_scraping_recent_part,
    run_full_scraping_process
)
from src.functions.etl_functions import run_full_etl_process, run_verification_and_correction
from src.utils.constants import PATHS
from src.utils.logging_config import setup_logging
from src.gateways.browser_manager import browser_manager

async def run_async_sync_process(logger, case_status):
    """
    Proceso de sincronización completamente asíncrono
    """
    TMP_FOLDER = PATHS["tmp_path"]
    try:
        pathlib.Path(TMP_FOLDER).mkdir(exist_ok=True)
        logger.info(f"Carpeta temporal '{TMP_FOLDER}' creada o ya existe.")

        logger.info(f"Proceso de sync iniciado para status: {case_status.upper()}")

        try:
            rollbar.report_message(
                f"Proceso de sync iniciado (Status: {case_status.upper()})", 
                "info"
            )
        except Exception as e:
            logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

        # Ejecutar scraping completo de manera optimizada
        logger.info("Iniciando proceso de scraping optimizado...")
        await run_full_scraping_process(logger, case_status)

        logger.info("Iniciando proceso ETL principal (actualización de BD)...")
        run_full_etl_process(logger)
        logger.info("Proceso ETL principal finalizado.")

        if case_status.strip().lower() == 'active':
            logger.info("Iniciando proceso de verificación y corrección (solo para 'active')...")
            await run_verification_and_correction(logger)
            logger.info("Proceso de verificación y corrección finalizado.")
        else:
            logger.info(f"Omitiendo 'run_verification_and_correction' para status: '{case_status}'.")

        logger.info(f"Proceso de sync finalizado con ÉXITO para status: {case_status.upper()}")
        
        try:
            rollbar.report_message(
                f"Proceso de sync finalizado con ÉXITO (Status: {case_status.upper()})", 
                "info"
            )
        except Exception as e:
            logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

    except Exception as e:
        logger.critical(f"Error en proceso de sync: {e}", exc_info=True)
        try:
            rollbar.report_exc_info()
        except Exception as re:
            logger.error(f"No se pudo reportar excepción a Rollbar: {re}")
        raise
    finally:
        # Limpiar navegador
        await browser_manager.stop()
        
        # Limpiar carpeta temporal
        try:
            if os.path.exists(TMP_FOLDER):
                shutil.rmtree(TMP_FOLDER)
                logger.info(f"Carpeta temporal '{TMP_FOLDER}' y todo su contenido han sido eliminados.")
        except OSError as e:
            logger.error(f"Error eliminando carpeta '{TMP_FOLDER}': {e.strerror}")

def run_sync_process(logger, case_status):
    """
    Función de entrada para ejecutar el proceso de sincronización
    """
    asyncio.run(run_async_sync_process(logger, case_status))