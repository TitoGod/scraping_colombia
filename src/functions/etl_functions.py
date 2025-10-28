import os
import sys
import pandas as pd
import uuid
import json
import rollbar
from datetime import datetime
from src.gateways.database_gateway import DatabaseManager
from src.utils.data_normalizer import DataNormalizer
from src.gateways.scraping_gateway import run_scraping_for_missing_requests
from src.utils.constants import PATHS, S3_PATHS
from src.gateways.s3_gateway import S3Manager

JSON_FOLDER_PATH = PATHS["tmp_path"]

def run_full_etl_process(logger):
    """Orquesta el proceso ETL completo, procesando los JSON en lotes (uno por uno)."""
    try:
        logger.info("Iniciando proceso ETL principal por lotes...")
        db_params = {
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASS"),
            "host": os.getenv("PG_HOST"),
            "port": os.getenv("PG_PORT"),
            "database": os.getenv("PG_DB")
        }
        table_name = os.getenv("TABLE")
        if not all(db_params.values()) or not table_name:
            logger.critical("Faltan variables de entorno de BD. Terminando ETL.")
            sys.exit(1)
        if not os.path.isdir(JSON_FOLDER_PATH):
            logger.critical(f"La carpeta JSON '{JSON_FOLDER_PATH}' no existe. Terminando ETL.")
            sys.exit(1)

        normalizer = DataNormalizer(raw_data_folder=JSON_FOLDER_PATH, logger=logger)
        db_manager = DatabaseManager(db_params, table_name, logger=logger)
        
        json_file_list = normalizer.get_json_file_list()
        if not json_file_list:
            logger.warning(f"No se encontraron archivos JSON en '{JSON_FOLDER_PATH}'. Terminando ETL.")
            return

        total_inserts = 0
        total_updates = 0
        report_data_global = []
        
        db_cols_list = None

        for i, file_path in enumerate(json_file_list):
            logger.info(f"--- Procesando Lote {i+1}/{len(json_file_list)}: {os.path.basename(file_path)} ---")
            
            new_data = normalizer.normalize_single_file(file_path)
            if not new_data:
                logger.warning(f"El archivo {file_path} no produjo datos. Omitiendo lote.")
                continue
                
            df_new = pd.DataFrame(new_data)
            df_new.drop_duplicates(subset=['request_number'], keep='first', inplace=True)
            df_new.set_index("request_number", inplace=True)
            
            request_numbers_in_lote = df_new.index.tolist()
            df_db = db_manager.fetch_records_by_request_numbers(request_numbers_in_lote)
            if not df_db.empty:
                df_db.drop_duplicates(subset=['request_number'], keep='first', inplace=True)
                df_db.set_index("request_number", inplace=True)

            for df in [df_new, df_db]:
                for col in ['filing_date', 'expiration_date']:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')

            logger.info("Comparando y clasificando registros del lote...")
            report_data_lote, records_to_insert_indices, records_to_update_indices = [], [], []
            all_request_numbers_lote = df_new.index
            
            for req_num in all_request_numbers_lote:
                changed, columns_changed = False, []
                in_db = not df_db.empty and req_num in df_db.index
                
                if not in_db:
                    columns_changed.append("NEW_RECORD")
                    records_to_insert_indices.append(req_num)
                else:
                    new_row, db_row = df_new.loc[req_num], df_db.loc[req_num]
                    common_columns = df_new.columns.intersection(df_db.columns)
                    for col in common_columns:
                        val_new, val_db = str(new_row.get(col, '') or '').strip(), str(db_row.get(col, '') or '').strip()
                        if val_new != val_db:
                            changed = True
                            columns_changed.append(col)
                    if changed: 
                        records_to_update_indices.append(req_num)
                        
                if columns_changed: 
                    report_data_lote.append({"request_number": req_num, "changed": True, "columns_changed": ", ".join(columns_changed)})

            logger.info(f"Lote comparado: {len(records_to_insert_indices)} nuevos, {len(records_to_update_indices)} modificados.")
            report_data_global.extend(report_data_lote)
            total_inserts += len(records_to_insert_indices)
            total_updates += len(records_to_update_indices)
            
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
                
                if db_cols_list is None:
                    logger.info("Obteniendo lista de columnas de la DB por primera vez...")
                    if not df_db.empty:
                        db_cols_list = df_db.columns.tolist()
                    else:
                        temp_df = db_manager.fetch_records_by_request_numbers(["-1"])
                        db_cols_list = temp_df.columns.tolist()

                cols_to_pass = [col for col in df_to_update.columns if col in db_cols_list or col == 'updated_at' or col == 'request_number']
                db_manager.update_records(df_to_update[cols_to_pass])
            
            logger.info(f"--- Lote {i+1} procesado y guardado en la DB. ---")

        logger.info("Todos los lotes procesados. Generando reporte CSV global...")
        csv_report_path = f"change_report_{datetime.now().strftime('%Y-%m-%d')}.csv"
        pd.DataFrame(report_data_global).to_csv(csv_report_path, index=False, encoding="utf-8")
        logger.info(f"Reporte CSV global exportado a '{csv_report_path}'")
        
        try:
            s3_manager = S3Manager(bucket_name=S3_PATHS["bucket_name"], logger=logger)
            s3_manager.upload_file(csv_report_path, S3_PATHS["reports_folder"])
        except Exception as e:
            logger.error(f"Fallo al subir reporte CSV a S3: {e}")
            rollbar.report_exc_info()

        try:
            rollbar.report_message(
                f"ETL: Base de datos actualizada ({total_inserts} nuevos, {total_updates} modificados) - PROCESADO EN LOTES",
                "info"
            )
        except Exception as e:
            logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")
            
        logger.info("Proceso ETL por lotes finalizado exitosamente.")
    
    except Exception as e:
        logger.critical(f"Un error inesperado ocurrió en el proceso ETL por lotes: {e}", exc_info=True)
        try:
            rollbar.report_exc_info()
        except:
            logger.error("No se pudo reportar el error de ETL a Rollbar.")
        sys.exit(1)

def compare_json_vs_db_and_generate_csv(logger):
    """Compares 'request_number' from JSONs vs. the DB and generates a CSV with missing ones."""
    logger.info("Starting comparison of JSONs vs. Database for active records...")

    try:
        rollbar.report_message(
            "Iniciando Comparación (JSONs vs DB) para corrección de faltantes",
            "info"
        )
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

    db_params = {"user": os.getenv("PG_USER"), "password": os.getenv("PG_PASS"), "host": os.getenv("PG_HOST"), "port": os.getenv("PG_PORT"), "database": os.getenv("PG_DB")}
    table_name = os.getenv("TABLE")
    if not all(db_params.values()) or not table_name:
        logger.critical("Missing database environment variables. Cannot continue with comparison.")
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
    
    s3_manager = S3Manager(bucket_name=S3_PATHS["bucket_name"], logger=logger)
    s3_manager.upload_file(csv_path, S3_PATHS["reports_folder"])
    
    return csv_path

def update_statuses_from_json(json_path, logger):
    """Reads a JSON with updated statuses and applies them to the database."""
    logger.info(f"Starting status update from file '{json_path}'...")

    try:
        rollbar.report_message(
            f"Iniciando actualización de DB (Corrección) desde: {json_path}",
            "info"
        )
    except Exception as e:
        logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")

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
        rollbar.report_exc_info()
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

        try:
            rollbar.report_message(
                f"Actualización de DB (Corrección) finalizada ({len(records_for_db)} records actualizados)",
                "success"
            )
        except Exception as e:
            logger.warning(f"No se pudo reportar mensaje a Rollbar: {e}")
            
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