import re
import pandas as pd
import numpy as np
import requests as req
from logging import Logger
import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.configs.settings import settings

class AMPECO_Downtime_Data_Importer:    

    def __init__(self):
        self.url_socket_downtime_list=settings.url_socket_downtime_list
        self.api_token = settings.api_token
        self.logger = Logger("ampeco_api_socket_downtime_fetcher.py")
    
    def fetch_downtime_data(self, last_update_date=None) -> pd.DataFrame:
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}"
        }

        all_pages = []
        cursor = None
        per_page = 100

        try:
            while True:
                params = {"per_page": per_page}
                if cursor:
                    params["cursor"] = cursor
                if last_update_date is not None:
                    params["filter[lastUpdatedAfter]"] = last_update_date.isoformat()

                response = req.get(
                    self.url_socket_downtime_list,
                    headers=headers,
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()

                page_data = data.get("data", [])
                if not page_data:
                    break

                all_pages.append(pd.DataFrame(page_data))  # acumula cada página

                cursor = data.get("meta", {}).get("next_cursor")
                print(f"Fetched {len(page_data)} records | next_cursor={cursor}")

                if not cursor:
                    break

            return pd.concat(all_pages, ignore_index=True) if all_pages else pd.DataFrame()
        

        except Exception as e:
            print(f"Error fetching downtime list: {e}")
            return pd.DataFrame()
'''
runner = AMPECO_Downtime_Data_Importer()
print("Iniciando a coleta de Downtime...")
df = runner.fetch_downtime_data()'''


