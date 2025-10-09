import os
import sys
import pandas as pd
import uuid
import json
from datetime import datetime
from src.gateways.database_gateway import DatabaseManager
from src.utils.data_normalizer import DataNormalizer
from src.gateways.scraping_gateway import run_scraping_for_missing_requests
from src.utils.constants import PATHS

JSON_FOLDER_PATH = PATHS["tmp_path"]

def run_full_etl_process(logger):
    """Main function that orchestrates the entire ETL process."""
    try:
        logger.info("Starting main ETL process...")
        db_params = {
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASS"),
            "host": os.getenv("PG_HOST"),
            "port": os.getenv("PG_PORT"),
            "database": os.getenv("PG_DB")
        }
        table_name = os.getenv("TABLE")
        if not all(db_params.values()) or not table_name:
            logger.critical("Missing database environment variables. Terminating ETL process.")
            sys.exit(1)
        if not os.path.isdir(JSON_FOLDER_PATH):
            logger.critical(f"The JSON folder '{JSON_FOLDER_PATH}' does not exist. Terminating ETL process.")
            sys.exit(1)

        normalizer = DataNormalizer(raw_data_folder=JSON_FOLDER_PATH, logger=logger)
        new_data = normalizer.combine_and_normalize_jsons()
        if not new_data:
            logger.warning(f"No data found in the folder '{JSON_FOLDER_PATH}'. Terminating ETL.")
            return
        df_new = pd.DataFrame(new_data)
        df_new.drop_duplicates(subset=['request_number'], keep='first', inplace=True)

        db_manager = DatabaseManager(db_params, table_name, logger=logger)
        df_db = db_manager.fetch_all_records()
        if not df_db.empty:
            df_db.drop_duplicates(subset=['request_number'], keep='first', inplace=True)

        df_new.set_index("request_number", inplace=True)
        if not df_db.empty:
            df_db.set_index("request_number", inplace=True)

        for df in [df_new, df_db]:
            for col in ['filing_date', 'expiration_date']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')

        logger.info("Step 3: Comparing and classifying records...")
        report_data, records_to_insert_indices, records_to_update_indices = [], [], []
        all_request_numbers = set(df_new.index).union(set(df_db.index if not df_db.empty else []))
        
        for req_num in all_request_numbers:
            changed, columns_changed, in_new, in_db = False, [], req_num in df_new.index, not df_db.empty and req_num in df_db.index
            if in_new and not in_db:
                columns_changed.append("NEW_RECORD")
                records_to_insert_indices.append(req_num)
            elif in_new and in_db:
                new_row, db_row = df_new.loc[req_num], df_db.loc[req_num]
                common_columns = df_new.columns.intersection(df_db.columns)
                for col in common_columns:
                    val_new, val_db = str(new_row.get(col, '') or '').strip(), str(db_row.get(col, '') or '').strip()
                    if val_new != val_db:
                        changed = True
                        columns_changed.append(col)
                if changed: records_to_update_indices.append(req_num)
            if columns_changed: report_data.append({"request_number": req_num, "changed": True, "columns_changed": ", ".join(columns_changed)})

        logger.info(f"Comparison finished: {len(records_to_insert_indices)} new, {len(records_to_update_indices)} modified.")
        
        csv_report_path = f"change_report_{datetime.now().strftime('%Y-%m-%d')}.csv"
        pd.DataFrame(report_data).to_csv(csv_report_path, index=False, encoding="utf-8")
        logger.info(f"Step 4.1: Exported CSV report to '{csv_report_path}'")
        
        df_to_insert = df_new.loc[records_to_insert_indices].copy()
        if not df_to_insert.empty:
            df_to_insert['id'] = [str(uuid.uuid4()) for _ in range(len(df_to_insert))]
            df_to_insert.reset_index(inplace=True)
            all_db_cols = ["id", "request_number", "registry_number", "denomination", "logo_url", "logo", "filing_date", "expiration_date", "status", "holder", "niza_class", "gazette_number", "updated_at", "badger_country"]
            df_to_insert = df_to_insert[[col for col in all_db_cols if col in df_to_insert.columns]]
            for col in ['filing_date', 'expiration_date']:
                if col in df_to_insert.columns: df_to_insert.loc[df_to_insert[col] == '', col] = None
        db_manager.insert_records(df_to_insert)
        
        df_to_update = df_new.loc[records_to_update_indices].copy().reset_index()
        if not df_to_update.empty:
            for col in ['filing_date', 'expiration_date']:
                if col in df_to_update.columns: df_to_update.loc[df_to_update[col] == '', col] = None
            db_cols_list = df_db.columns.tolist() if not df_db.empty else []
            cols_to_pass = [col for col in df_to_update.columns if col in db_cols_list or col == 'updated_at' or col == 'request_number']
            db_manager.update_records(df_to_update[cols_to_pass])
            
        logger.info("Main ETL process finished successfully.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred in the ETL process: {e}", exc_info=True)
        sys.exit(1)

def compare_json_vs_db_and_generate_csv(logger):
    """Compares 'request_number' from JSONs vs. the DB and generates a CSV with missing ones."""
    logger.info("Starting comparison of JSONs vs. Database for active records...")
    db_params = {"user": os.getenv("PG_USER"), "password": os.getenv("PG_PASS"), "host": os.getenv("PG_HOST"), "port": os.getenv("PG_PORT"), "database": os.getenv("PG_DB")}
    table_name = os.getenv("TABLE")
    if not all(db_params.values()) or not table_name:
        logger.critical("Missing environment variables. Cannot continue with comparison.")
        return None
    
    normalizer = DataNormalizer(JSON_FOLDER_PATH, logger)
    json_requests = normalizer.get_all_request_numbers_from_jsons()

    db_manager = DatabaseManager(db_params, table_name, logger)
    db_active_requests = db_manager.fetch_active_request_numbers()

    missing_requests = sorted(list(db_active_requests - json_requests))
    
    if not missing_requests:
        logger.info("Excellent! No discrepancies found. The database is in sync with the JSONs.")
        return None
        
    logger.warning(f"Found {len(missing_requests)} active records in the DB that are not in the JSONs.")
    
    csv_path = os.path.join(JSON_FOLDER_PATH, "missing_records.csv")
    df_missing = pd.DataFrame(missing_requests, columns=["missing_request_number"])
    df_missing.to_csv(csv_path, index=False, encoding="utf-8")
    
    logger.info(f"Generated file '{csv_path}' with the missing records.")
    return csv_path

def update_statuses_from_json(json_path, logger):
    """Reads a JSON with updated statuses and applies them to the database."""
    logger.info(f"Starting status update from file '{json_path}'...")
    db_params = {"user": os.getenv("PG_USER"), "password": os.getenv("PG_PASS"), "host": os.getenv("PG_HOST"), "port": os.getenv("PG_PORT"), "database": os.getenv("PG_DB")}
    table_name = os.getenv("TABLE")
    if not all(db_params.values()) or not table_name:
        logger.critical("Missing environment variables. Cannot update the DB.")
        return

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data_to_update = json.load(f)
    except FileNotFoundError:
        logger.error(f"The results file '{json_path}' was not found.")
        return
    except json.JSONDecodeError:
        logger.error(f"Error decoding the results JSON '{json_path}'.")
        return
    
    status_mapping = {
        "VIGENTE": "VIGENTE", "CANCELADA": "CANCELADA", "ANULADO CONSEJO DE ESTADO": "ANULADA",
        "CADUCADO": "VENCIDA", "RENUNCIA TOTAL": "RENUNCIA_TOTAL", "NEGADA": "NEGADA",
        "DESISTIDA": "DESISTIDA", "ABANDONADA": "ABANDONADA", "BAJO EXAMEN DE FONDO": "EXAMEN_DE_FONDO",
        "BAJO EXAMEN FORMAL": "EXAMEN_DE_FORMA", "PUBLICADA": "EN_GACETA", "CON OPOSICION": "OPOSICION"
    }

    records_for_db = []
    for record in data_to_update:
        if record.get('error') is None and record.get('extracted_status'):
            normalized_status = status_mapping.get(record['extracted_status'].upper())
            if normalized_status:
                records_for_db.append({
                    "request_number": record['request_number'],
                    "status": normalized_status
                })
            else:
                logger.warning(f"Status '{record['extracted_status']}' for '{record['request_number']}' has no valid mapping. It will be skipped.")
    
    if records_for_db:
        db_manager = DatabaseManager(db_params, table_name, logger)
        db_manager.update_record_statuses(records_for_db)
    else:
        logger.info("No valid records found in the JSON to update in the database.")

async def run_verification_and_correction(logger):
    """
    Orchestrates the flow of finding, re-scraping, and updating missing records.
    """
    logger.info("\n\n")
    logger.info("=============================================================")
    logger.info("======= START OF VERIFICATION AND CORRECTION PROCESS =======")
    logger.info("=============================================================")

    missing_csv_path = compare_json_vs_db_and_generate_csv(logger)

    if not missing_csv_path:
        logger.info("No CSV of missing records was generated. The verification process has ended.")
        return

    results_json_path = await run_scraping_for_missing_requests(missing_csv_path, logger)

    if not results_json_path:
        logger.warning("The JSON file with results from scraping missing records was not generated.")
        return

    update_statuses_from_json(results_json_path, logger)

    logger.info("=============================================================")
    logger.info("====== VERIFICATION AND CORRECTION PROCESS FINISHED ======")
    logger.info("=============================================================")