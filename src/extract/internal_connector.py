import pandas as pd
import os
from src.extract.base_connector import BaseConnector


class InternalDataConnector(BaseConnector):
    """
    Connector for internal data sources (CSV files, Excel files, etc.)
    """

    def __init__(self, file_path, source_type="csv", **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.source_type = source_type.lower()

    def extract(self):
        """
        Load internal data from file
        """
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Internal data file not found: {self.file_path}")

        if self.source_type == "csv":
            df = pd.read_csv(self.file_path, engine="python", on_bad_lines="skip")
        elif self.source_type in ["xlsx", "xls"]:
            df = pd.read_excel(self.file_path)
        elif self.source_type == "json":
            df = pd.read_json(self.file_path)
        else:
            raise ValueError(f"Unsupported internal data type: {self.source_type}")

        # Add source metadata
        df["_source_type"] = "internal"
        df["_file_path"] = self.file_path

        return self.validate_output(df)