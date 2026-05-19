import requests
import pandas as pd
from src.extract.base_connector import BaseConnector


class APIConnector(BaseConnector):

    def __init__(self, url, headers=None, params=None):
        super().__init__()
        self.url = url
        self.headers = headers
        self.params = params

    def extract(self):

        response = requests.get(
            self.url,
            headers=self.headers,
            params=self.params,
            timeout=30
        )

        if response.status_code != 200:
            raise Exception(f"API failed with status {response.status_code}")

        data = response.json()

        # normalize structure safely
        df = pd.json_normalize(data)

        return self.validate_output(df)