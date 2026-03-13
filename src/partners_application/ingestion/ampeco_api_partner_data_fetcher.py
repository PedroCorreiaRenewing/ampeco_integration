import re
import pandas as pd
import numpy as np
import requests as req
from logging import Logger
import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.configs.settings import settings

#AMPECO API classifies charging points as chargers and EVSEs as sockets within chargers.
class AMPECO_Partners_Data_Importer:    

    def __init__(self):
        self.url_partners_list=settings.url_partners_list
        self.api_token = settings.api_token
        self.logger = Logger("ampeco_api_partners_fetcher.py")

    
    #fetch all partners from AMPECO API with pagination
    def fetch_partners(self) -> pd.DataFrame:
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}"
        }

        all_dfs = [] # Lista para guardar os pedaços de dados
        cursor = None
        per_page = 100

        try:
            while True:
                params = {"cursor": cursor, "per_page": per_page} if cursor else {"cursor": "null", "per_page": per_page}
                response = req.get(self.url_partners_list, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                page_data = data.get("data", [])
                if not page_data:
                    break

                # Em vez de yield, adicionamos o DataFrame à lista
                all_dfs.append(pd.DataFrame(page_data))

                cursor = data.get("meta", {}).get("next_cursor")
                if not cursor:
                    break 

            # Consolida tudo em um único DataFrame ao final
            if all_dfs:
                partners_df = pd.concat(all_dfs, ignore_index=True)
                print(f"Total Partners fetched: {len(partners_df)}")
                return partners_df
            else:
                return pd.DataFrame()

        except Exception as e:
            print(f"Error fetching partners: {e}")
            return pd.DataFrame()
        
       

        