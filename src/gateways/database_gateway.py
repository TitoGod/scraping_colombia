import os
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime

class DatabaseManager:
    """Class to manage all interactions with the PostgreSQL database."""
    def __init__(self, db_params, table_name, logger):
        self.db_params = db_params
        self.table_name = table_name
        self.logger = logger
        self.ACTIVE_STATES = (
            "EXAMEN_DE_FORMA", "SUSPENDIDA", "EN_GACETA", "EXAMEN_DE_FONDO",
            "OPOSICION", "CERTIFICADA_Y_ENVIADA", "IRREGULAR", "VIGENTE", "PROTEGIDA"
        )

    def fetch_active_request_numbers(self):
        """Fetches all 'request_number' with an active status from the DB."""
        self.logger.info(f"Fetching active 'request_number' from '{self.table_name}'...")
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            with conn.cursor() as cur:
                query = f'SELECT "request_number" FROM {self.table_name} WHERE "status" IN %s;'
                cur.execute(query, (self.ACTIVE_STATES,))
                results = {row[0] for row in cur.fetchall()}
                self.logger.info(f"Fetched {len(results)} active records from the database.")
                return results
        except Exception as e:
            self.logger.critical(f"CRITICAL error fetching active 'request_numbers': {e}")
            return set()
        finally:
            if conn: conn.close()

    def update_record_statuses(self, records_to_update):
        """Updates the status of a list of records in the DB."""
        if not records_to_update:
            self.logger.info("No missing record statuses to update.")
            return

        self.logger.info(f"Updating the status of {len(records_to_update)} records in the DB...")
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            with conn.cursor() as cur:
                update_query = f'UPDATE {self.table_name} SET "status" = %s, "updated_at" = %s WHERE "request_number" = %s;'
                data_tuples = [
                    (rec['status'], datetime.now().strftime("%Y-%m-%d %H:%M:%S"), rec['request_number'])
                    for rec in records_to_update if rec.get('status')
                ]
                if not data_tuples:
                    self.logger.warning("No records with a valid status to update.")
                    return
                psycopg2.extras.execute_batch(cur, update_query, data_tuples)
                conn.commit()
                self.logger.info(f"Successfully completed update of {len(data_tuples)} statuses.")
        except Exception as e:
            self.logger.error(f"Error updating statuses in the database: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def fetch_all_records(self):
        columns_to_fetch = ["request_number", "registry_number", "denomination", "logo_url", "filing_date", "expiration_date", "status", "holder", "niza_class", "gazette_number"]
        columns_str = ", ".join([f'"{col}"' for col in columns_to_fetch])
        self.logger.info(f"Step 2: Connecting to DB and fetching ALL data from '{self.table_name}'...")
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            query = f"SELECT {columns_str} FROM {self.table_name} WHERE badger_country = 'COLOMBIA';"
            df = pd.read_sql_query(query, conn)
            self.logger.info(f"Fetched {len(df)} records from the database.")
            return df
        except Exception as e:
            self.logger.critical(f"CRITICAL error connecting to or fetching data from the database: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    def insert_records(self, df_to_insert):
        if df_to_insert.empty:
            self.logger.info("Step 5.1: No new records to insert.")
            return
        self.logger.info(f"Step 5.1: Inserting {len(df_to_insert)} new records...")
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            with conn.cursor() as cur:
                cols = df_to_insert.columns.tolist()
                cols_str = ", ".join([f'"{col}"' for col in cols])
                vals_str = ", ".join(["%s"] * len(cols))
                insert_query = f"INSERT INTO {self.table_name} ({cols_str}) VALUES ({vals_str});"
                data_tuples = [tuple(row) for row in df_to_insert.to_numpy()]
                psycopg2.extras.execute_batch(cur, insert_query, data_tuples)
                conn.commit()
                self.logger.info("Insertion completed successfully.")
        except Exception as e:
            self.logger.error(f"Error inserting into the database: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def update_records(self, df_to_update):
        if df_to_update.empty:
            self.logger.info("Step 5.2: No existing records to update.")
            return
        self.logger.info(f"Step 5.2: Updating {len(df_to_update)} existing records...")
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            with conn.cursor() as cur:
                cols_to_update = [col for col in df_to_update.columns if col not in ['request_number']]
                set_clause = ", ".join([f'"{col}" = %s' for col in cols_to_update])
                update_query = f'UPDATE {self.table_name} SET {set_clause} WHERE "request_number" = %s;'
                data_tuples = []
                for _, row in df_to_update.iterrows():
                    update_values = [row[col] for col in cols_to_update]
                    update_values.append(row['request_number'])
                    data_tuples.append(tuple(update_values))
                psycopg2.extras.execute_batch(cur, update_query, data_tuples)
                conn.commit()
                self.logger.info("Update completed successfully.")
        except Exception as e:
            self.logger.error(f"Error updating the database: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()