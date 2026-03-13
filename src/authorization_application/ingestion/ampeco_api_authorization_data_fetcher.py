import re
import pandas as pd
import numpy as np
import requests as req
from logging import Logger
import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.configs.settings import settings

#AMPECO API classifies charging points as chargers and EVSEs as sockets within chargers.
class AMPECO_Authorization_Data_Importer:    

    def __init__(self):
        self.url_users_list= settings.url_users_list
        self.url_user_groups_list=settings.url_user_group_list
        self.url_authorizations_list=settings.url_authorization_list
        self.api_token = settings.api_token
        self.logger = Logger("ampeco_api_authorization_fetcher.py")

    
    #fetch all authorizations from AMPECO API with pagination
    def fetch_authorizations_paginated(self, last_update_date=None) -> pd.DataFrame:
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}"
        }

        all_authorizations = []
        cursor = None  #first cursor is None
        per_page = 100  #max established by AMPECO API

        try:
            while True:
                params = {"cursor": cursor, "per_page": per_page} if cursor else {"cursor": "null", "per_page": per_page}

                if last_update_date is not None:
                    
                    params["filter[lastUpdatedAfter]"] = last_update_date.isoformat()

                response = req.get(self.url_authorizations_list, headers=headers, params=params,timeout=30)
                response.raise_for_status()
                data = response.json()

                page_data = data.get("data", [])
                if not page_data:
                    break

                yield pd.DataFrame(page_data)

                cursor = data.get("meta", {}).get("next_cursor")
                print(f"Fetched {len(page_data)} sessions | next_cursor={cursor}")

                if not cursor:
                    break                    
            
                
            authorizations_df = pd.DataFrame(all_authorizations)
            print(f"Total Authorizations fetched: {len(authorizations_df)}") #
            print(authorizations_df.head())
            
            return authorizations_df
        except Exception as e:
            print(f"Error fetching authorizations: {e}")
            #self.logger.error(f"Error fetching authorizations: {e}")
            return pd.DataFrame()
        
    '''
        
    def fetch_authorizations(self) -> pd.DataFrame:
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}"
        }

        all_authorizations = []
        cursor = None 
        per_page = 100 
        
        # 1. Inicializa o contador
        call_count = 0
        max_calls = 3

        try:
            while True:
                params = {"cursor": cursor, "per_page": per_page} if cursor else {"cursor": "null", "per_page": per_page}
                response = req.get(self.url_authorizations_list, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                page_data = data.get("data", [])
                all_authorizations.extend(page_data)

                # 2. Incrementa o contador após o processamento da página
                call_count += 1
                cursor = data.get("meta", {}).get("next_cursor")
                
                print(f"Chamada {call_count}: Buscou {len(page_data)} itens, próximo cursor: {cursor}")

                # 3. Condição de parada: Sem cursor OU atingiu o limite de 3 chamadas
                if not cursor or call_count >= max_calls:
                    if call_count >= max_calls:
                        print(f"Interrompendo: Limite de {max_calls} chamadas atingido.")
                    break

            authorizations_df = pd.DataFrame(all_authorizations)
            print(authorizations_df.head())
            authorizations_df.to_excel("authorizations_sample.xlsx", index=False)  # Salva um sample em Excel para análise
            print(f"Total de autorizações buscadas: {len(authorizations_df)}")
            return authorizations_df
            
        except Exception as e:
            print(f"Error fetching authorizations: {e}")
            return pd.DataFrame()'''
            
       

        