import os
import json
import re
from datetime import datetime

class DataNormalizer:
    """Class responsible for processing and normalizing JSON files."""
    def __init__(self, raw_data_folder, logger):
        self.folder_path = raw_data_folder
        self.logger = logger
        self.month_mapping = {
            "ene.": "01", "feb.": "02", "mar.": "03", "abr.": "04", "may.": "05", "jun.": "06",
            "jul.": "07", "ago.": "08", "sept.": "09", "oct.": "10", "nov.": "11", "dic.": "12"
        }
        self.status_mapping = {
            "registrada": "VIGENTE", "cancelada": "CANCELADA", "anulado consejo de estado": "ANULADA",
            "caducado": "VENCIDA", "renuncia total": "RENUNCIA_TOTAL", "negada": "NEGADA",
            "desistida": "DESISTIDA", "abandonada": "ABANDONADA", "bajo examen de fondo": "EXAMEN_DE_FONDO",
            "bajo examen formal": "EXAMEN_DE_FORMA", "publicada": "EN_GACETA", "con oposición": "OPOSICION"
        }

    def _format_date(self, date_str):
        if not date_str: return None
        try:
            day, spa_month, year = date_str.split(" ")
            eng_month = self.month_mapping.get(spa_month.lower())
            if not eng_month: raise ValueError("Invalid month")
            return f"{year}-{eng_month}-{day.zfill(2)}"
        except (ValueError, AttributeError):
            self.logger.warning(f"Could not format date: '{date_str}'. It will be set to null.")
            return None

    def _normalize_holder(self, holder):
        if not holder: return ""
        holder_list = holder if isinstance(holder, list) else [holder]
        normalized_list = [re.sub(r'\s+', ' ', name.replace(";", ",")).strip().upper() for name in holder_list]
        return "; ".join(normalized_list)
    
    def get_all_request_numbers_from_jsons(self):
        """Reads all JSONs and returns a set of unique request_numbers."""
        request_numbers = set()
        for filename in os.listdir(self.folder_path):
            if filename.endswith(".json"):
                file_path = os.path.join(self.folder_path, filename)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        for entry in data:
                            if entry and 'request_number' in entry:
                                request_numbers.add(entry['request_number'])
                except (json.JSONDecodeError, TypeError):
                    self.logger.error(f"Error reading or processing JSON file: '{file_path}'. It will be skipped.")
        self.logger.info(f"Found {len(request_numbers)} unique 'request_number' values in JSON files.")
        return request_numbers

    def combine_and_normalize_jsons(self):
        """Reads all JSON files in the folder, combines, and normalizes them."""
        combined_data = []
        for filename in os.listdir(self.folder_path):
            if filename.endswith(".json"):
                file_path = os.path.join(self.folder_path, filename)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        combined_data.extend(data if isinstance(data, list) else [data])
                except json.JSONDecodeError:
                    self.logger.error(f"Error decoding JSON file: '{file_path}'. It will be skipped.")
                except Exception as e:
                    self.logger.error(f"Unexpected error reading '{file_path}': {e}. It will be skipped.")

        self.logger.info(f"Step 1: Merged a total of {len(combined_data)} records from the '{self.folder_path}' folder.")
        final_data = []
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for entry in combined_data:
            request_number = entry.get("request_number", "")
            if not request_number:
                self.logger.warning(f"Record skipped due to missing 'request_number': {entry}")
                continue
            
            final_entry = {
                "request_number": request_number,
                "registry_number": entry.get("registry_number", ""),
                "denomination": entry.get("denomination", ""),
                "logo_url": entry.get("logo_url", ""),
                "logo": f"https://gazette-primary-assets.s3.amazonaws.com/logos/colombia/records/{request_number.replace('/', '_')}.jpeg" if entry.get("logo_url") else "",
                "filing_date": self._format_date(entry.get("filing_date", "")),
                "expiration_date": self._format_date(entry.get("expiration_date", "")),
                "status": self.status_mapping.get(str(entry.get("status", "")).lower(), entry.get("status")),
                "holder": self._normalize_holder(entry.get("holder", "")),
                "niza_class": entry.get("niza_class", ""),
                "gazette_number": entry.get("gazette_number", ""),
                "updated_at": updated_at,
                "badger_country": "COLOMBIA"
            }
            final_data.append(final_entry)
            
        self.logger.info(f"A total of {len(final_data)} records have been normalized.")
        return final_data