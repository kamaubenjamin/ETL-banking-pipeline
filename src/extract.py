from src.utils import log_progress
log_progress("Extract module loaded")
import pandas as pd
import requests
from bs4 import BeautifulSoup

def extract(url, table_attribs):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    tables = soup.find_all('tbody')
    if not tables:
     raise Exception("No tables found on the page")

    rows = tables[0].find_all('tr')

    if not rows:
     raise Exception("No rows found in the table")

    for row in rows:
        col = row.find_all('td')
        if len(col) > 2:
            data_dict = {
                "Name": col[1].text.strip(),
                "MC_USD_Billion": col[2].text.strip()
            }
            df1 = pd.DataFrame([data_dict])
            df = pd.concat([df, df1], ignore_index=True)
    return df