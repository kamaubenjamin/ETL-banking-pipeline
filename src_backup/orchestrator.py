import sqlite3
import time

from src.extract.extract import run_extraction
from src.transform.engine import TransformEngine
from src.load import load_to_csv, load_to_db
from src.audit import AuditLogger, DataValidator


class ETLPipeline:

    def __init__(self, config):
        self.config = config
        self.audit = AuditLogger()
        self.validator = DataValidator()

    def run(self, source_type, uploaded_df=None, mode=None, selector=None,
            rules=None, load_option="CSV"):

        start_time = time.time()

        result = {
            "extract": {},
            "transform": {},
            "load": {},
            "data": None,
            "shape": None,
            "execution_time": None
        }

        # -----------------------
        # EXTRACT
        # -----------------------
        extract_start = time.time()
        df = run_extraction(
            source_type=source_type,
            config=self.config,
            uploaded_df=uploaded_df,
            mode=mode,
            selector=selector
        )
        extract_duration = time.time() - extract_start

        if df is None or df.empty:
            self.audit.log_extraction(source_type, 0, "failed", "No data extracted", extract_duration)
            raise Exception("No data extracted")

        self.audit.log_extraction(source_type, len(df), "success", None, extract_duration)

        result["extract"] = {
            "rows": df.shape[0],
            "cols": df.shape[1],
            "status": "Success"
        }

        # -----------------------
        # TRANSFORM
        # -----------------------
        transform_start = time.time()
        df_original = df.copy()
        engine = TransformEngine(df)
        df = engine.apply(rules or [])
        transform_duration = time.time() - transform_start

        self.audit.log_transformation(
            len(df_original),
            len(df),
            [r.get("type") for r in (rules or [])],
            "success",
            None,
            transform_duration
        )

        result["transform"] = {
            "rows": df.shape[0],
            "cols": df.shape[1],
            "status": "Success"
        }

        # -----------------------
        # LOAD
        # -----------------------
        load_start = time.time()
        load_destination = "None"
        
        if load_option == "CSV":
            load_to_csv(df, self.config.csv_path)
            load_destination = "CSV"

        elif load_option == "Database":
            conn = sqlite3.connect(self.config.db_name)
            load_to_db(df, conn, self.config.table_name)
            conn.close()
            load_destination = "Database"

        elif load_option == "Both":
            load_to_csv(df, self.config.csv_path)
            conn = sqlite3.connect(self.config.db_name)
            load_to_db(df, conn, self.config.table_name)
            conn.close()
            load_destination = "Both"

        load_duration = time.time() - load_start
        self.audit.log_load(load_destination, len(df), "success", None, load_duration)

        result["load"] = {
            "status": "Success",
            "rows": df.shape[0]
        }

        result["data"] = df
        result["shape"] = df.shape
        result["execution_time"] = round(time.time() - start_time, 2)

        return result