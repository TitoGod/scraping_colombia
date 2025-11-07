import asyncio
import pathlib
import shutil
import os
import rollbar
import multiprocessing
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from src.functions.scraping_functions import (
    run_niza_class_scraping, 
    run_scraping_historical_part, 
    run_scraping_recent_part
)
from src.functions.etl_functions import run_full_etl_process, run_verification_and_correction
from src.utils.constants import PATHS
from src.utils.logging_config import setup_logging


def _scrape_worker_1(case_status):
    """
    Worker Proceso 1: Ejecuta Niza + Scraping Histórico (1900-2014).
    """
    logger = setup_logging()
    logger.info("--- [Proceso 1] INICIANDO (Niza + Fechas Históricas) ---")
    
    async def worker_1_main_tasks():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36",
                viewport={"width": 1280, "height": 900}
            )
            page = await context.new_page()
            logger.info("--- [Proceso 1] Navegador Chromium iniciado. ---")
            
            try:
                logger.info("--- [Proceso 1] Iniciando scraping Clases Niza... ---")
                await run_niza_class_scraping(page, logger, context_tag="[Worker-1]")
                logger.info("--- [Proceso 1] Scraping Clases Niza FINALIZADO. ---")
                
                logger.info("--- [Proceso 1] Iniciando scraping Fechas Históricas... ---")
                await run_scraping_historical_part(page, logger, case_status, context_tag="[Worker-1]")
                logger.info("--- [Proceso 1] Scraping Fechas Históricas FINALIZADO. ---")
            
            finally:
                await browser.close()
                logger.info("--- [Proceso 1] Navegador Chromium cerrado. ---")

    try:
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
    Worker Proceso 2: Ejecuta Scraping Reciente (2019-Presente).
    """
    logger = setup_logging()
    logger.info(f"--- [Proceso 2] INICIANDO (Fechas Recientes, Status: {case_status}) ---")
    
    async def worker_2_main_task():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36",
                viewport={"width": 1280, "height": 900}
            )
            page = await context.new_page()
            logger.info("--- [Proceso 2] Navegador Chromium iniciado. ---")

            try:
                await run_scraping_recent_part(page, logger, case_status, context_tag="[Worker-2]")
            
            finally:
                await browser.close()
                logger.info("--- [Proceso 2] Navegador Chromium cerrado. ---")

    try:
        asyncio.run(worker_2_main_task())
        logger.info("--- [Proceso 2] FINALIZADO (Fechas Recientes) ---")
        
    except Exception as e:
        logger.critical(f"--- [Proceso 2] FALLÓ: {e}", exc_info=True)
        try:
            rollbar.report_exc_info()
        except Exception as re:
            logger.error(f"No se pudo reportar excepción de Proceso 2 a Rollbar: {re}")


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

        logger.info("Iniciando procesos de scraping en paralelo (divididos)...")
        
        process1 = multiprocessing.Process(
            target=_scrape_worker_1,
            args=(case_status,),
            name="Worker-1"
        )
        process2 = multiprocessing.Process(
            target=_scrape_worker_2,
            args=(case_status,),
            name="Worker-2"
        )

        # Iniciar ambos procesos
        process1.start()
        process2.start()

        # Esperar a que ambos procesos terminen
        process1.join()
        logger.info("--- Proceso 1 (Niza + Histórico) ha finalizado. ---")
        process2.join()
        logger.info("--- Proceso 2 (Reciente) ha finalizado. ---")

        logger.info("Ambos procesos de scraping han terminado.")

        logger.info("Iniciando proceso ETL principal (actualización de BD)...")
        run_full_etl_process(logger)
        logger.info("Proceso ETL principal finalizado.")

        logger.info("Iniciando proceso de verificación y corrección...")
        
        async def verification_task():
            await run_verification_and_correction(logger)
            
        asyncio.run(verification_task())
        logger.info("Proceso de verificación y corrección finalizado.")

        logger.info(f"Proceso de sync finalizado con ÉXITO para status: {case_status.upper()}")
        
        try:
            rollbar.report_message(
                f"Proceso de sync finalizado con ÉXITO (Status: {case_status.upper()})", 
                "info"
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