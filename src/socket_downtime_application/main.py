import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.socket_downtime_application.ingestion.ampeco_api_socket_downtime_fetcher import AMPECO_Downtime_Data_Importer
from src.socket_downtime_application.transform.ampeco_api_socket_downtime_transformation import SocketDowntimeDataTransformer
from src.socket_downtime_application.load.db_uploader import DBUploader
from src.repository.db_functions import PostgresInteraction    
from src.configs.settings import settings
from sqlalchemy import create_engine
import pandas as pd

class SocketDowntimePipeline:
    def __init__(self):
        self.fetcher = AMPECO_Downtime_Data_Importer()
        self.transformer = SocketDowntimeDataTransformer()
        self.uploader = DBUploader(settings.database_url)
        engine = create_engine(settings.database_url)
        self.db_interactor=PostgresInteraction(engine)
    def run(self):
        
        print("===== START AMPECO SOCKET DOWNTIME ETL =====")

        source_system_id = self.uploader.get_source_system_id("AMPECO")
        print(f"Source system ID for AMPECO: {source_system_id}")
        max_date = self.db_interactor.get_max_value(
            column_name="start_date",
            table_name="socket_downtime",
            where={"source_system_id": source_system_id}
        )
        print(f"Max start_date in DB for AMPECO socket downtime: {max_date}")

        fetch_from_date = (
            max_date - pd.Timedelta(days=4)
            if pd.notnull(max_date)
            else pd.Timestamp("2022-01-01")
        )
        
        # 1. FETCH
                
        print(f"Fetching socket downtime from: {fetch_from_date}...")
        socket_downtime_df = self.fetcher.fetch_downtime_data(fetch_from_date)        
        
        # 2. TRANSFORM
        
        socket_downtime_df = self.transformer.clean_socket_downtime(socket_downtime_df)
        print(socket_downtime_df.head())
        
        
        
        socket_downtime_df['source_system_id'] = source_system_id
        
        socket_pk_df = self.db_interactor.fetch(
                table_name="ev_charger_socket",
                columns=["id", "source_id"],
                where={
                    "source_system_id": source_system_id,
                    "source_id": tuple(socket_downtime_df["socket_source_id"].astype(str).tolist())
                },
                as_dataframe=True
            )

        socket_pk_df=socket_pk_df.rename(columns={"id": "socket_id", "source_id": "socket_source_match_id"})

        socket_downtime_df["socket_source_id"] = socket_downtime_df["socket_source_id"].astype("Int64")
        socket_pk_df["socket_source_match_id"] = socket_pk_df["socket_source_match_id"].astype("Int64")

        socket_downtime_df = socket_downtime_df.merge(
            socket_pk_df, left_on="socket_source_id", right_on="socket_source_match_id", how="left"
        )      
        socket_downtime_df = socket_downtime_df.drop(columns=["socket_source_match_id", "socket_source_id"], errors="ignore")
        socket_downtime_df=socket_downtime_df.dropna(subset=["socket_id"])
        socket_downtime_df=socket_downtime_df.drop_duplicates(subset=["socket_id", "start_date", "end_date"])
        socket_downtime_df = socket_downtime_df.sort_values(by=["source_id"])


        print("Uploading socket downtime to database...")   
           
        self.uploader.upsert_socket_downtime(socket_downtime_df)              
             
        print("===== ETL COMPLETED SUCCESSFULLY =====")

if __name__ == "__main__":
    SocketDowntimePipeline().run()