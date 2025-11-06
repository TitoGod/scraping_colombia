# scraping_functions.py (refactorizado)
import os
from datetime import date, datetime, timedelta
import calendar
import rollbar
import asyncio
from src.utils.constants import PATHS
from src.gateways.scraping_gateway import (
    scrape_by_date_range,
    scrape_by_niza_class
)
from src.gateways.browser_manager import browser_manager

DOWNLOADS_PATH = PATHS["tmp_path"]

async def run_scraping_tasks_concurrently(date_ranges, case_state, logger, max_concurrent=2):
    """Ejecuta tareas de scraping concurrentemente con límite"""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def run_with_semaphore(date_range):
        async with semaphore:
            context = await browser_manager.new_context()
            page = await context.new_page()
            try:
                await scrape_by_date_range(page, date_range['start'], date_range['end'], case_state, logger)
            finally:
                await context.close()
    
    tasks = [run_with_semaphore(date_range) for date_range in date_ranges]
    await asyncio.gather(*tasks, return_exceptions=True)

async def run_scraping_by_year_interval(page, start_date_str, end_date_str, year_interval, case_state, logger):
    """Versión optimizada que genera tareas concurrentes"""
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
    current_date = start_date
    
    date_ranges = []
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

        if not os.path.exists(output_filename):
            date_ranges.append({
                'start': interval_start_str,
                'end': interval_end_str
            })

        current_date = interval_end_dt + timedelta(days=1)
    
    if date_ranges:
        await run_scraping_tasks_concurrently(date_ranges, case_state, logger)

async def run_scraping_by_month(page, start_date_str, end_date_str, case_state, logger):
    """Versión optimizada para scraping mensual"""
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
    current_date = start_date
    
    date_ranges = []
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

        if not os.path.exists(output_filename):
            date_ranges.append({
                'start': month_start_str,
                'end': month_end_str
            })

        current_date = month_end_dt + timedelta(days=1)
    
    if date_ranges:
        await run_scraping_tasks_concurrently(date_ranges, case_state, logger)

async def run_scraping_by_week(page, start_date_str, end_date_str, case_state, logger):
    """Versión optimizada para scraping semanal"""
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
    current_date = start_date
    
    date_ranges = []
    while current_date <= end_date:
        week_start_dt = current_date
        week_end_dt = min(current_date + timedelta(days=6), end_date)
        week_start_str = week_start_dt.strftime("%d/%m/%Y")
        week_end_str = week_end_dt.strftime("%d/%m/%Y")
        
        start_safe = week_start_str.replace("/", "_")
        end_safe = week_end_str.replace("/", "_")
        tag = 'ACTIVE' if case_state.strip().lower() == 'active' else 'INACTIVE'
        output_filename = f'{DOWNLOADS_PATH}{start_safe}_{end_safe}_{tag}.json'

        if not os.path.exists(output_filename):
            date_ranges.append({
                'start': week_start_str,
                'end': week_end_str
            })

        current_date = week_end_dt + timedelta(days=1)
    
    if date_ranges:
        await run_scraping_tasks_concurrently(date_ranges, case_state, logger)

async def run_scraping_by_day(page, start_date_str, end_date_str, case_state, logger):
    """Versión optimizada para scraping diario"""
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
    current_date = start_date
    
    date_ranges = []
    while current_date <= end_date:
        day_str = current_date.strftime("%d/%m/%Y")
        day_safe = day_str.replace("/", "_")
        tag = 'ACTIVE' if case_state.strip().lower() == 'active' else 'INACTIVE'
        output_filename = f'{DOWNLOADS_PATH}{day_safe}_{day_safe}_{tag}.json'

        if not os.path.exists(output_filename):
            date_ranges.append({
                'start': day_str,
                'end': day_str
            })
        
        current_date += timedelta(days=1)
    
    if date_ranges:
        await run_scraping_tasks_concurrently(date_ranges, case_state, logger, max_concurrent=1)

async def run_niza_class_scraping_concurrent(logger, context_tag="[Scraping]"):
    """Ejecuta scraping de clases Niza concurrentemente"""
    try:
        rollbar.report_message(f"{context_tag} Iniciando scraping por Niza class (1-45)", "info")
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

    async def scrape_single_niza_class(niza_class):
        context = await browser_manager.new_context()
        page = await context.new_page()
        try:
            output_filename = f'{DOWNLOADS_PATH}niza_{niza_class}_1900_1900_ACTIVE.json'
            if not os.path.exists(output_filename):
                logger.info(f"=== Scraping Niza Class ({'ACTIVE'}): {niza_class} ===")
                await scrape_by_niza_class(page, niza_class, logger)
        except Exception as e:
            logger.error(f"Error en Niza Class {niza_class}: {e}")
        finally:
            await context.close()

    tasks = [scrape_single_niza_class(niza_class) for niza_class in range(1, 45)]
    await asyncio.gather(*tasks, return_exceptions=True)

    try:
        rollbar.report_message(f"{context_tag} Scraping por Niza class finalizado con éxito", "info")
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

async def run_scraping_historical_part(logger, case_status, context_tag="[Scraping]"):
    """Parte histórica ejecutada concurrentemente"""
    logger.info(f"--- Iniciando Scraping Parte 1 (Histórico) para Status: '{case_status.upper()}' ---")
    try:
        rollbar.report_message(
            f"{context_tag} Iniciando scraping Parte 1 (Histórico, Status: {case_status.upper()})", 
            "info"
        )
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

    # Crear contexto para tareas históricas
    context = await browser_manager.new_context()
    page = await context.new_page()
    
    try:
        if case_status.strip().lower() == 'active':
            logger.info("--- Usando rangos de fecha para 'ACTIVE' ---")
            await run_scraping_by_year_interval(page, "02/01/1900", "31/12/1970", 71, case_status, logger)
            await run_scraping_by_year_interval(page, "01/01/1971", "31/12/1985", 5, case_status, logger)
            await run_scraping_by_year_interval(page, "01/01/1986", "31/12/1988", 1, case_status, logger)
            await run_scraping_by_month(page, "01/01/1989", "30/11/2014", case_status, logger)
            await run_scraping_by_week(page, "01/12/2014", "31/12/2018", case_status, logger)
        
        elif case_status.strip().lower() == 'inactive':
            logger.info("--- Usando rangos de fecha para 'INACTIVE' ---")
            await run_scraping_by_year_interval(page, "02/01/1900", "31/12/1960", 61, case_status, logger)
            await run_scraping_by_year_interval(page, "01/01/1961", "31/12/1970", 10, case_status, logger)
            await run_scraping_by_year_interval(page, "01/01/1971", "31/12/1980", 1, case_status, logger)
            await run_scraping_by_month(page, "01/01/1981", "31/12/2002", case_status, logger)
        else:
            logger.error(f"Estado de caso '{case_status}' no reconocido para rangos de fecha históricos.")
    finally:
        await context.close()
    
    logger.info("--- Scraping Parte 1 (Histórico) FINALIZADO ---")

async def run_scraping_recent_part(logger, case_status, context_tag="[Scraping]"):
    """Parte reciente ejecutada concurrentemente"""
    logger.info(f"--- Iniciando Scraping Parte 2 (Reciente) para Status: '{case_status.upper()}' ---")
    current_date = date.today().strftime('%d/%m/%Y')
    
    try:
        rollbar.report_message(
            f"{context_tag} Iniciando scraping Parte 2 (Reciente, Status: {case_status.upper()})", 
            "info"
        )
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

    # Crear contexto para tareas recientes
    context = await browser_manager.new_context()
    page = await context.new_page()
    
    try:
        if case_status.strip().lower() == 'active':
            logger.info("--- Usando rangos de fecha para 'ACTIVE' ---")
            await run_scraping_by_week(page, "01/01/2019", "27/12/2022", case_status, logger)
            await run_scraping_by_day(page, "28/12/2022", "31/12/2022", case_status, logger)
            await run_scraping_by_week(page, "01/01/2023", current_date, case_status, logger)
        
        elif case_status.strip().lower() == 'inactive':
            logger.info("--- Usando rangos de fecha para 'INACTIVE' ---")
            await run_scraping_by_month(page, "01/01/2003", "30/11/2010", case_status, logger)
            await run_scraping_by_week(page, "01/12/2010", "31/12/2010", case_status, logger)
            await run_scraping_by_month(page, "01/01/2011", "30/11/2011", case_status, logger)
            await run_scraping_by_week(page, "01/12/2011", "31/12/2011", case_status, logger)
            await run_scraping_by_month(page, "01/01/2012", current_date, case_status, logger)
        else:
            logger.error(f"Estado de caso '{case_status}' no reconocido para rangos de fecha recientes.")
    finally:
        await context.close()

    logger.info("--- Scraping Parte 2 (Reciente) FINALIZADO ---")

async def run_full_scraping_process(logger, case_status):
    """Orquesta todas las etapas de scraping usando asyncio.gather"""
    logger.info("=========================================================")
    logger.info("========== START OF DATE-BASED SCRAPING ==========")
    logger.info("=========================================================")
    
    # Ejecutar scraping histórico y de Niza concurrentemente si es active
    if case_status.strip().lower() == 'active':
        await asyncio.gather(
            run_scraping_historical_part(logger, case_status),
            run_niza_class_scraping_concurrent(logger),
            return_exceptions=True
        )
    else:
        await run_scraping_historical_part(logger, case_status)
    
    await run_scraping_recent_part(logger, case_status)

    logger.info("=======================================================")
    logger.info("========== DATE-BASED SCRAPING FINISHED ==========")
    logger.info("=======================================================")