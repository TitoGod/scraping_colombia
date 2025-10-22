import asyncio
import pathlib
import shutil
import os
import rollbar
from src.functions.scraping_functions import run_full_scraping_process, run_niza_class_scraping
from src.functions.etl_functions import run_full_etl_process, run_verification_and_correction
from src.utils.constants import PATHS

def run_sync_process(logger, case_status):
    """
    Contains the core business logic for the synchronization process,
    including setup, execution, and cleanup.
    """
    TMP_FOLDER = PATHS["tmp_path"]
    try:
        pathlib.Path(TMP_FOLDER).mkdir(exist_ok=True)
        logger.info(f"Temporary folder '{TMP_FOLDER}' created or already exists.")

        logger.info(f"Sync process initiated for status: {case_status.upper()}")

        try:
            rollbar.report_message(
                f"Proceso de sync iniciado (Status: {case_status.upper()})", 
                "info"
            )
        except Exception as e:
            logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

        async def async_tasks():
            await run_niza_class_scraping(logger)
            await run_full_scraping_process(logger, case_status)
            await run_verification_and_correction(logger)

        asyncio.run(async_tasks())

        run_full_etl_process(logger)

        logger.info(f"Sync process finished successfully for status: {case_status.upper()}")
        
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
                logger.info(f"Temporary folder '{TMP_FOLDER}' and all its contents have been deleted.")
        except OSError as e:
            logger.error(f"Error deleting folder '{TMP_FOLDER}': {e.strerror}")