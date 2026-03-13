import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.authorization_application.ingestion.ampeco_api_authorization_data_fetcher import AMPECO_Authorization_Data_Importer
from src.authorization_application.transform.authorization_data_transformer import AuthorizationDataTransformer
from src.authorization_application.load.db_uploader import DBUploader
from src.repository.db_functions import PostgresInteraction    
from src.configs.settings import settings
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


class AuthorizationsPipeline:

    def __init__(self):
        self.fetcher = AMPECO_Authorization_Data_Importer()
        self.transformer = AuthorizationDataTransformer()
        self.uploader = DBUploader(settings.database_url)
        engine = create_engine(settings.database_url)
        self.db_interactor=PostgresInteraction(engine)

    def run(self):
        
        print("===== START AMPECO AUTHORIZATION ETL =====")

        
        # 1. FETCH

        max_date = self.db_interactor.get_max_value(
            column_name="last_update_date",
            table_name="authorizations_history"
        )

        fetch_from_date = (
            max_date - pd.Timedelta(days=4)
            if pd.notnull(max_date)
            else pd.Timestamp("2022-01-01")
        )

        print(f"Fetching authorizations from: {fetch_from_date}")
        
        for i, authorizations_df in enumerate(
            self.fetcher.fetch_authorizations_paginated(fetch_from_date), start=1
        ):
            print(f"Processing batch {i} with {len(authorizations_df)} authorizations")

            if authorizations_df.empty:
                print("Batch vazio — skipping consumption fetch")
                continue


        #authorizations_df = self.fetcher.fetch_authorizations()         
            print(authorizations_df.head())
        
            # 2. TRANSFORM
            
            authorizations_df = self.transformer.clean_authorization(authorizations_df)        

            print(authorizations_df.head())
            
            
            source_system_id = self.uploader.get_source_system_id("AMPECO")
            
            authorizations_df['source_system_id'] = source_system_id
            authorizations_df=authorizations_df.sort_values(by="source_id", ascending=True)


            def get_valid_ids(df, column_name):
                # Filtra nulos e converte para string, removendo o ".0" caso seja float
                valid_ids = df[column_name].dropna().unique()
                # Converte para int e depois str para garantir que IDs como 1.0 virem "1"
                clean_ids = [str(int(float(x))) for x in valid_ids if str(x) not in ['0', '0.0', '']]
                return tuple(clean_ids) if clean_ids else (None,)            
            
            
            valid_charger_ids = get_valid_ids(authorizations_df, "charger_id")
            charger_pk_df = self.db_interactor.fetch(
                    table_name="ev_charger",
                    columns=["id", "source_id"],
                    where={
                        "source_system_id": source_system_id,
                        "source_id": valid_charger_ids
                    },
                    as_dataframe=True
                )
            
            authorizations_df = (
                    authorizations_df
                    .merge(charger_pk_df, left_on="charger_id", right_on="source_id", how="left")
                    .rename(columns={"id": "charger_id_new"})
            )

            authorizations_df = authorizations_df.drop(columns=["charger_id"], errors="ignore").rename(columns={"charger_id_new": "charger_id"})

            print(authorizations_df.columns)
            authorizations_df = authorizations_df.drop(columns=["source_id_y"], errors="ignore").rename(columns={"source_id_x": "source_id"})

            valid_socket_ids = get_valid_ids(authorizations_df, "socket_id")
            socket_pk_df = self.db_interactor.fetch(
                    table_name="ev_charger_socket",
                    columns=["id", "source_id"],
                    where={
                        "source_system_id": source_system_id,
                        "source_id": valid_socket_ids
                    },
                    as_dataframe=True
                )
            
            authorizations_df = (
                    authorizations_df
                    .merge(socket_pk_df, left_on="socket_id", right_on="source_id", how="left")
                    .rename(columns={"id": "socket_id_new"})
            )

            print(authorizations_df.columns)
            authorizations_df = authorizations_df.drop(columns=["source_id_y"], errors="ignore").rename(columns={"source_id_x": "source_id"})

            authorizations_df = authorizations_df.drop(columns=["socket_id"], errors="ignore").rename(columns={"socket_id_new": "socket_id"})

            valid_user_ids = get_valid_ids(authorizations_df, "user_id")
            user_pk_df = self.db_interactor.fetch(
                    table_name="users",
                    columns=["id", "source_id"],
                    where={
                        "source_system_id": source_system_id,
                        "source_id": valid_user_ids
                    },
                    as_dataframe=True
                )
            
            user_pk_df = user_pk_df.rename(columns={"source_id": "user_source_match_id"})

            authorizations_df["user_id"] = authorizations_df["user_id"].astype("Int64")
            user_pk_df["user_source_match_id"] = user_pk_df["user_source_match_id"].astype("Int64")   
            
            authorizations_df = (
                    authorizations_df
                    .merge(user_pk_df, left_on="user_id", right_on="user_source_match_id", how="left")
                    .rename(columns={"id": "user_id_new"})
            )

            print(authorizations_df.columns)

            authorizations_df = authorizations_df.drop(columns=["user_id", "user_source_match_id"], errors="ignore").rename(columns={"user_id_new": "user_id"})

            authorizations_df = authorizations_df.replace({np.nan: None})
            
#            print("Uploading authorizations to database...")
            self.uploader.upsert_inventory_authorization(authorizations_df)

            print(f"Batch {i} completed ✅")                   
             

        print("===== ETL COMPLETED SUCCESSFULLY =====")


if __name__ == "__main__":
    AuthorizationsPipeline().run()
