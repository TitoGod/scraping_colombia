import os
from datetime import date, datetime, timedelta
import calendar
import rollbar
from src.utils.constants import PATHS
from src.gateways.scraping_gateway import (
    scrape_by_date_range,
    scrape_by_niza_class
)

DOWNLOADS_PATH = PATHS["tmp_path"]

async def run_scraping_by_year_interval(start_date_str, end_date_str, year_interval, case_state, logger):
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
    current_date = start_date
    while current_date <= end_date:
        interval_start_dt = current_date
        try:
            end_year = interval_start_dt.year + year_interval
            next_start_date = interval_start_dt.replace(year=end_year)
            interval_end_dt = next_start_date - timedelta(days=1)
        except ValueError:
            end_year = interval_start_dt.year + year_interval
            next_start_date = interval_start_dt.replace(year=end_year, day=28)
            interval_end_dt = next_start_date - timedelta(days=1)

        interval_end_dt = min(interval_end_dt, end_date)
        interval_start_str = interval_start_dt.strftime("%d/%m/%Y")
        interval_end_str = interval_end_dt.strftime("%d/%m/%Y")
        start_safe = interval_start_str.replace("/", "_")
        end_safe = interval_end_str.replace("/", "_")
        tag = 'ACTIVE' if case_state.strip().lower() == 'active' else 'INACTIVE'
        output_filename = f'{DOWNLOADS_PATH}{start_safe}_{end_safe}_{tag}.json'

        if os.path.exists(output_filename):
            logger.info(f"File '{output_filename}' already exists. Skipping interval.")
        else:
            logger.info(f"=== Scraping {year_interval}-year interval ({tag}): {interval_start_str} -> {interval_end_str} ===")
            await scrape_by_date_range(interval_start_str, interval_end_str, case_state, logger)
        current_date = interval_end_dt + timedelta(days=1)

async def run_scraping_by_month(start_date_str, end_date_str, case_state, logger):
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
    current_date = start_date
    while current_date <= end_date:
        month_start_dt = datetime(current_date.year, current_date.month, 1)
        _, last_day = calendar.monthrange(current_date.year, current_date.month)
        month_end_dt = min(datetime(current_date.year, current_date.month, last_day), end_date)
        month_start_str = month_start_dt.strftime("%d/%m/%Y")
        month_end_str = month_end_dt.strftime("%d/%m/%Y")
        start_safe = month_start_str.replace("/", "_")
        end_safe = month_end_str.replace("/", "_")
        tag = 'ACTIVE' if case_state.strip().lower() == 'active' else 'INACTIVE'
        output_filename = f'{DOWNLOADS_PATH}{start_safe}_{end_safe}_{tag}.json'

        if os.path.exists(output_filename):
            logger.info(f"File '{output_filename}' already exists. Skipping month {current_date.strftime('%Y-%m')}.")
        else:
            logger.info(f"=== Scraping month ({tag}): {month_start_str} -> {month_end_str} ===")
            await scrape_by_date_range(month_start_str, month_end_str, case_state, logger)
        current_date = month_end_dt + timedelta(days=1)

async def run_scraping_by_week(start_date_str, end_date_str, case_state, logger):
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
    current_date = start_date
    while current_date <= end_date:
        week_start_dt = current_date
        week_end_dt = min(current_date + timedelta(days=6), end_date)
        week_start_str = week_start_dt.strftime("%d/%m/%Y")
        week_end_str = week_end_dt.strftime("%d/%m/%Y")
        start_safe = week_start_str.replace("/", "_")
        end_safe = week_end_str.replace("/", "_")
        tag = 'ACTIVE' if case_state.strip().lower() == 'active' else 'INACTIVE'
        output_filename = f'{DOWNLOADS_PATH}{start_safe}_{end_safe}_{tag}.json'

        if os.path.exists(output_filename):
            logger.info(f"File '{output_filename}' already exists. Skipping week {week_start_str} - {week_end_str}.")
        else:
            logger.info(f"=== Scraping week ({tag}): {week_start_str} -> {week_end_str} ===")
            await scrape_by_date_range(week_start_str, week_end_str, case_state, logger)
        current_date = week_end_dt + timedelta(days=1)

async def run_scraping_by_day(start_date_str, end_date_str, case_state, logger):
    """
    Scrapes data day by day for a given date range.
    Ideal for periods with a high volume of records to avoid exceeding limits.
    """
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
    current_date = start_date
    while current_date <= end_date:
        day_str = current_date.strftime("%d/%m/%Y")
        day_safe = day_str.replace("/", "_")
        tag = 'ACTIVE' if case_state.strip().lower() == 'active' else 'INACTIVE'
        output_filename = f'{DOWNLOADS_PATH}{day_safe}_{day_safe}_{tag}.json'

        if os.path.exists(output_filename):
            logger.info(f"File '{output_filename}' already exists. Skipping day {day_str}.")
        else:
            logger.info(f"=== Scraping day ({tag}): {day_str} ===")
            await scrape_by_date_range(day_str, day_str, case_state, logger)
        
        current_date += timedelta(days=1)

# --- NUEVA FUNCIÓN PARTE 1 ---
async def run_scraping_historical_part(logger, case_status, context_tag="[Scraping]"):
    """
    Ejecuta la primera parte (histórica) del scraping por fechas (1900-2014).
    """
    logger.info(f"--- Iniciando Scraping Parte 1 (Histórico) para Status: '{case_status.upper()}' ---")
    try:
        rollbar.report_message(
            f"{context_tag} Iniciando scraping Parte 1 (Histórico, Status: {case_status.upper()})", 
            "info"
        )
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

    await run_scraping_by_year_interval("02/01/1900", "31/12/1970", 71, case_status, logger)
    await run_scraping_by_year_interval("01/01/1971", "31/12/1975", 5, case_status, logger)
    await run_scraping_by_year_interval("01/01/1976", "31/12/1980", 5, case_status, logger)
    await run_scraping_by_year_interval("01/01/1981", "31/12/1985", 5, case_status, logger)
    await run_scraping_by_year_interval("01/01/1986", "31/12/1986", 1, case_status, logger)
    await run_scraping_by_year_interval("01/01/1987", "31/12/1987", 1, case_status, logger)
    await run_scraping_by_year_interval("01/01/1988", "31/12/1988", 1, case_status, logger)
    await run_scraping_by_month("01/01/1989", "30/11/2014", case_status, logger)
    
    logger.info("--- Scraping Parte 1 (Histórico) FINALIZADO ---")

    try:
        rollbar.report_message(
            f"{context_tag} Finalizado scraping Parte 1 (Histórico, Status: {case_status.upper()})", 
            "info"
        )
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

# --- NUEVA FUNCIÓN PARTE 2 ---
async def run_scraping_recent_part(logger, case_status, context_tag="[Scraping]"):
    """
    Ejecuta la segunda parte (reciente y más intensiva) del scraping por fechas (2014-Presente).
    """
    logger.info(f"--- Iniciando Scraping Parte 2 (Reciente) para Status: '{case_status.upper()}' ---")
    current_date = date.today().strftime('%d/%m/%Y')
    
    try:
        rollbar.report_message(
            f"{context_tag} Iniciando scraping Parte 2 (Reciente, Status: {case_status.upper()})", 
            "info"
        )
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

    await run_scraping_by_week("01/12/2014", "27/12/2022", case_status, logger)
    await run_scraping_by_day("28/12/2022", "31/12/2022", case_status, logger)
    await run_scraping_by_week("01/01/2023", current_date, case_status, logger)
    
    logger.info("--- Scraping Parte 2 (Reciente) FINALIZADO ---")

    try:
        rollbar.report_message(
            f"{context_tag} Finalizado scraping Parte 2 (Reciente, Status: {case_status.upper()})", 
            "info"
        )
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

# --- FUNCIÓN PRINCIPAL MODIFICADA ---
async def run_full_scraping_process(logger, case_status):
    """
    Orquesta todas las etapas de scraping por rango de fechas (ahora modularizado).
    """
    logger.info("=========================================================")
    logger.info("========== START OF DATE-BASED SCRAPING ==========")
    logger.info("=========================================================")
    
    # Llama a las dos partes en secuencia
    await run_scraping_historical_part(logger, case_status)
    await run_scraping_recent_part(logger, case_status)

    logger.info("=======================================================")
    logger.info("========== DATE-BASED SCRAPING FINISHED ==========")
    logger.info("=======================================================")

async def run_niza_class_scraping(logger, context_tag="[Scraping]"):
    """Executes scraping for all Niza classes (1-44)."""

    try:
        rollbar.report_message(f"{context_tag} Iniciando scraping por Niza class (1-45)", "info")
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

    for niza_class in range(1, 45):
        output_filename = f'{DOWNLOADS_PATH}niza_{niza_class}_1900_1900_ACTIVE.json'
        if os.path.exists(output_filename):
            logger.info(f"File '{output_filename}' already exists. Skipping Niza class {niza_class}.")
        else:
            logger.info(f"=== Scraping Niza Class ({'ACTIVE'}): {niza_class} ===")
            await scrape_by_niza_class(niza_class, logger, headless=True)

    try:
        rollbar.report_message(f"{context_tag} Scraping por Niza class finalizado con éxito", "info")
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")