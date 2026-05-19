import pandas as pd
from src.extract.base_connector import BaseConnector


class DataFrameConnector(BaseConnector):

    def _init_(self, df):
        super()._init_()
        self.df = df

    def extract(self):

        if self.df is None:
            raise ValueError("DataFrameConnector received None dataframe")

        return self.validate_output(self.df.copy())