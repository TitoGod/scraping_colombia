# src/functions/sync_orchestrator.py

import asyncio
import pathlib
import shutil
import os
# Añade glob para buscar archivos
import glob
from datetime import datetime
from src.functions.scraping_functions import run_full_scraping_process, run_niza_class_scraping
from src.functions.etl_functions import run_full_etl_process, run_verification_and_correction
from src.utils.constants import PATHS

def run_sync_process(logger, case_status):
    """
    Contains the core business logic for the synchronization process,
    including setup, execution, and cleanup.
    """
    TMP_FOLDER = PATHS["tmp_path"]
    # 1. Define la carpeta de reportes permanente
    REPORTS_FOLDER = "reports/"

    try:
        pathlib.Path(TMP_FOLDER).mkdir(exist_ok=True)
        # Asegúrate de que la carpeta de reportes también exista
        pathlib.Path(REPORTS_FOLDER).mkdir(exist_ok=True)
        logger.info(f"Temporary folder '{TMP_FOLDER}' created or already exists.")

        # ... (el resto de tu lógica de scraping y ETL se mantiene igual)
        async def async_tasks():
            await run_niza_class_scraping(logger)
            await run_full_scraping_process(logger, case_status)
            await run_verification_and_correction(logger)

        asyncio.run(async_tasks())
        run_full_etl_process(logger)

        logger.info(f"Sync process finished successfully for status: {case_status.upper()}")

        # 2. Mueve los reportes ANTES de que se borre la carpeta tmp
        logger.info("Moving reports to a permanent directory...")

        # Mueve el reporte de registros faltantes
        missing_report_path = os.path.join(TMP_FOLDER, "missing_records.csv")
        if os.path.exists(missing_report_path):
            shutil.move(missing_report_path, os.path.join(REPORTS_FOLDER, "missing_records.csv"))
            logger.info("Moved missing_records.csv")

        # Mueve el reporte de cambios (usando un patrón para encontrarlo)
        today_str = datetime.now().strftime('%Y-%m-%d')
        change_report_pattern = f"change_report_{today_str}.csv"
        if os.path.exists(change_report_pattern):
            shutil.move(change_report_pattern, os.path.join(REPORTS_FOLDER, change_report_pattern))
            logger.info(f"Moved {change_report_pattern}")

    finally:
        try:
            if os.path.exists(TMP_FOLDER):
                shutil.rmtree(TMP_FOLDER)
                logger.info(f"Temporary folder '{TMP_FOLDER}' and all its contents have been deleted.")
        except OSError as e:
            logger.error(f"Error deleting folder '{TMP_FOLDER}': {e.strerror}")