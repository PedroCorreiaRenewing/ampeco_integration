import re
import pandas as pd
import numpy as np
import requests as req
from logging import Logger
import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.configs.settings import settings

#AMPECO API classifies charging points as chargers and EVSEs as sockets within chargers.
class AMPECO_Users_Data_Importer:    

    def __init__(self):
        self.url_users_list= settings.url_users_list
        self.url_user_groups_list=settings.url_user_group_list
        self.api_token = settings.api_token
        self.logger = Logger("ampeco_api_users_fetcher.py")

    #fetch all users from AMPECO API with pagination
    def fetch_users(self) -> pd.DataFrame:
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}"
        }

        all_users = []
        cursor = None  #first cursor is None
        per_page = 100  #max established by AMPECO API

        try:
            while True:
                params = {"cursor": cursor, "per_page": per_page} if cursor else {"cursor": "null", "per_page": per_page}
                response = req.get(self.url_users_list, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                page_data = data.get("data", [])
                all_users.extend(page_data)

                #get the next cursor from the meta.next_cursor field
                cursor = data.get("meta", {}).get("next_cursor")
                print(f"Fetched {len(page_data)} items, next cursor: {cursor}")

                if not cursor:
                    break  #no more pages

            users_df = pd.DataFrame(all_users)
            print(users_df.head())
            print(f"Total users fetched: {len(users_df)}")
            #self.logger.info(f"Fetched {len(users_df)} users from Ampeco API.")
            return users_df
        except Exception as e:
            print(f"Error fetching users: {e}")
            #self.logger.error(f"Error fetching users: {e}")
            return pd.DataFrame()
        
    def fetch_user_groups(self) -> pd.DataFrame:
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}"
        }

        all_user_groups = []
        cursor = None  #first cursor is None
        per_page = 100  #max established by AMPECO API

        try:
            while True:
                params = {"cursor": cursor, "per_page": per_page} if cursor else {"cursor": "null", "per_page": per_page}
                response = req.get(self.url_user_groups_list, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                page_data = data.get("data", [])
                all_user_groups.extend(page_data)

                #get the next cursor from the meta.next_cursor field
                cursor = data.get("meta", {}).get("next_cursor")
                print(f"Fetched {len(page_data)} items, next cursor: {cursor}")

                if not cursor:
                    break  #no more pages

            user_groups_df = pd.DataFrame(all_user_groups)
            print(user_groups_df.head())
            print(f"Total user groups fetched: {len(user_groups_df)}")
            #self.logger.info(f"Fetched {len(user_groups_df)} user groups from Ampeco API.")
            return user_groups_df
        except Exception as e:
            print(f"Error fetching users: {e}")
            #self.logger.error(f"Error fetching users: {e}")
            return pd.DataFrame()
    '''
    def run(self):
        
        print("Fetching all users...")
        users_df = self.fetch_users()
        
        import re

        # Função para remover caracteres ilegais
        def clean_illegal_chars(val):
            if isinstance(val, str):
                # Remove caracteres de controle (ASCII 0-31), exceto tab, newline e carriage return
                return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\xff]', '', val)
            return val

        # Aplicar a limpeza a todas as colunas
        users_df = users_df.applymap(clean_illegal_chars)

        # Agora já pode exportar com segurança
        users_df.to_excel("all_users.xlsx", index=False)
        
        
        print("Fetching all user groups...")
        user_groups_df = self.fetch_user_groups()
        print(users_df)
        print(user_groups_df)
        
        

            

  
runner=AMPECO_Inventory_Importer()
runner.run()'''