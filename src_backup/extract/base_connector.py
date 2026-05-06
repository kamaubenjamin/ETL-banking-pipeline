# src/extract/base_connector.py

class BaseConnector:
    """
    Standard contract for all data connectors
    """

    def __init__(self, config=None, uploaded_df=None, mode=None, selector=None):
        self.config = config
        self.uploaded_df = uploaded_df
        self.mode = mode
        self.selector = selector

    def extract(self):
        """
        MUST return a pandas DataFrame
        """
        raise NotImplementedError("Connector must implement extract()")

    def validate_output(self, df): # can be overridden by child connectors for custom validation
        """
        Ensures connector returns valid DataFrame
        """
        import pandas as pd

        if df is None:
            raise Exception("Connector returned None")

        if not isinstance(df, pd.DataFrame):
            raise Exception("Connector must return a pandas DataFrame")

        if df.empty:
            raise Exception("Connector returned empty DataFrame")

        return df