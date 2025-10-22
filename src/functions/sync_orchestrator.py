import asyncio
import pathlib
import shutil
import os
import rollbar
import multiprocessing
# Importar las funciones específicas que usaremos en los workers
from src.functions.scraping_functions import (
    run_niza_class_scraping, 
    run_scraping_historical_part, 
    run_scraping_recent_part
)
from src.functions.etl_functions import run_full_etl_process, run_verification_and_correction
from src.utils.constants import PATHS
from src.utils.logging_config import setup_logging

# --- Funciones Worker para Multiprocessing ---

def _scrape_worker_1(case_status):
    """
    Worker Proceso 1: Ejecuta Niza + Scraping Histórico (1900-2014).
    """
    logger = setup_logging() # Inicializa el logger en este nuevo proceso
    logger.info("--- [Proceso 1] INICIANDO (Niza + Fechas Históricas) ---")
    try:
        async def worker_1_main_tasks():
            # 1. Tarea Niza
            logger.info("--- [Proceso 1] Iniciando scraping Clases Niza... ---")
            await run_niza_class_scraping(logger)
            logger.info("--- [Proceso 1] Scraping Clases Niza FINALIZADO. ---")
            
            # 2. Tarea Fechas Históricas
            logger.info("--- [Proceso 1] Iniciando scraping Fechas Históricas... ---")
            await run_scraping_historical_part(logger, case_status)
            logger.info("--- [Proceso 1] Scraping Fechas Históricas FINALIZADO. ---")
        
        # Ejecuta las tareas async de este worker
        asyncio.run(worker_1_main_tasks())
        logger.info("--- [Proceso 1] FINALIZADO (Niza + Fechas Históricas) ---")
        
    except Exception as e:
        logger.critical(f"--- [Proceso 1] FALLÓ: {e}", exc_info=True)
        try:
            rollbar.report_exc_info()
        except Exception as re:
            logger.error(f"No se pudo reportar excepción de Proceso 1 a Rollbar: {re}")

def _scrape_worker_2(case_status):
    """
    Worker Proceso 2: Ejecuta Scraping Reciente (2014-Presente).
    """
    logger = setup_logging() # Inicializa el logger en este nuevo proceso
    logger.info(f"--- [Proceso 2] INICIANDO (Fechas Recientes, Status: {case_status}) ---")
    try:
        async def worker_2_main_task():
            # 1. Tarea Fechas Recientes
            await run_scraping_recent_part(logger, case_status)

        # Ejecuta la tarea async de este worker
        asyncio.run(worker_2_main_task())
        logger.info("--- [Proceso 2] FINALIZADO (Fechas Recientes) ---")
        
    except Exception as e:
        logger.critical(f"--- [Proceso 2] FALLÓ: {e}", exc_info=True)
        try:
            rollbar.report_exc_info()
        except Exception as re:
            logger.error(f"No se pudo reportar excepción de Proceso 2 a Rollbar: {re}")

# --- Orquestador Principal (Modificado) ---

def run_sync_process(logger, case_status):
    """
    Contiene la lógica de negocio central para el proceso de sincronización,
    incluyendo configuración, ejecución paralela y limpieza.
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

        # --- 1. Ejecutar Scraping en Paralelo ---
        logger.info("Iniciando procesos de scraping en paralelo (divididos)...")
        
        # Pasa case_status a ambos workers
        process1 = multiprocessing.Process(target=_scrape_worker_1, args=(case_status,))
        process2 = multiprocessing.Process(target=_scrape_worker_2, args=(case_status,))

        # Iniciar ambos procesos
        process1.start()
        process2.start()

        # Esperar a que ambos procesos terminen
        process1.join()
        logger.info("--- Proceso 1 (Niza + Histórico) ha finalizado. ---")
        process2.join()
        logger.info("--- Proceso 2 (Reciente) ha finalizado. ---")

        logger.info("Ambos procesos de scraping han terminado.")

        # --- 2. Ejecutar ETL Principal ---
        # (Como solicitaste: después de scrapear, actualizar la BD)
        logger.info("Iniciando proceso ETL principal (actualización de BD)...")
        run_full_etl_process(logger)
        logger.info("Proceso ETL principal finalizado.")

        # --- 3. Ejecutar Verificación y Corrección ---
        # (Como solicitaste: *después* de la actualización principal de la BD)
        logger.info("Iniciando proceso de verificación y corrección...")
        
        async def verification_task():
            await run_verification_and_correction(logger)
            
        asyncio.run(verification_task())
        logger.info("Proceso de verificación y corrección finalizado.")

        logger.info(f"Proceso de sync finalizado con ÉXITO para status: {case_status.upper()}")
        
        try:
            rollbar.report_message(
                f"Proceso de sync finalizado con ÉXITO (Status: {case_status.upper()})", 
                "success"
            )
        except Exception as e:
            logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

    finally:
        try:
            if os.path.exists(TMP_FOLDER):
                shutil.rmtree(TMP_FOLDER)
                logger.info(f"Carpeta temporal '{TMP_FOLDER}' y todo su contenido han sido eliminados.")
        except OSError as e:
            logger.error(f"Error eliminando carpeta '{TMP_FOLDER}': {e.strerror}")