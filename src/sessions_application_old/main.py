import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.sessions_application.ingestion.ampeco_api_session_fetcher import AMPECO_Session_Importer
from src.sessions_application.transform.sessions_data_transformation import SessionsTransformer
from src.sessions_application.load.db_uploader import DBUploader
from src.repository.db_functions import PostgresInteraction    
from src.configs.settings import settings
from sqlalchemy import create_engine
import pandas as pd


class SessionsPipeline:

    def __init__(self):
        self.fetcher = AMPECO_Session_Importer()
        self.transformer = SessionsTransformer()
        self.uploader = DBUploader(settings.database_url)
        engine = create_engine(settings.database_url)
        self.db_interactor=PostgresInteraction(engine)

    def run(self):
        
        print("===== START AMPECO SESSIONS ETL =====")

        
        # 1. FETCH
                
        print("Fetching sessions...")
        max_date=self.db_interactor.get_max_value(
        column_name="start_date",
        table_name="ev_charger_session"
        )
        print(f"Max start_date in ev_charger_session table: {max_date}")

        max_date=max_date-pd.Timedelta(days=4) if pd.notnull(max_date) else None

    
        session_df = self.fetcher.fetch_sessions()
        
        
        print("Fetching sessions consumptions...")
        diff_day_session_consumption_df = self.fetcher.fetch_session_consumption(session_df)

        
        # 2. TRANSFORM AND LOAD
        
        session_clean = self.transformer.clean_session_data(session_df) 
        
        source_system_id = self.uploader.get_source_system_id("AMPECO")

        ev_charger_session_df, ev_charger_session_daily_df=self.transformer.build_inventory_tables(session_clean,diff_day_session_consumption_df)
        ev_charger_session_df['source_system_id'] = source_system_id
        ev_charger_session_df=ev_charger_session_df.sort_values(by="source_id", ascending=True)

        socket_id_pk_df = self.db_interactor.fetch(
        table_name="ev_charger_socket",
        columns=["id", "source_id"],
        as_dataframe=True
    )

        socket_id_pk_df["source_id"] = (
    pd.to_numeric(socket_id_pk_df["source_id"], errors="coerce")
    .astype("Int64"))
        
        ev_charger_session_df = ev_charger_session_df.merge(socket_id_pk_df, left_on="socket_source_id", right_on="source_id", how="left")
        ev_charger_session_df.drop(columns=["socket_source_id","source_id_y"], inplace=True)
        ev_charger_session_df = ev_charger_session_df.rename(columns={"id": "socket_id","source_id_x": "source_id"})
        ev_charger_session_df['socket_id'] = ev_charger_session_df['socket_id'].astype('Int64')         
        ev_charger_session_df['total_duration_min'] = ev_charger_session_df['total_duration_min'].astype('Float64')

        ev_charger_session_df=ev_charger_session_df.where(pd.notnull(ev_charger_session_df), None)

        print(ev_charger_session_df.head())

        #Insert ev_charger_session into DB
        print("Uploading ev_charger_session to database...")
        self.uploader.upsert_session(ev_charger_session_df) 

        sessions_pk_df = self.db_interactor.fetch(
        table_name="ev_charger_session",
        columns=["id", "source_id"],
        where={
        "source_system_id": source_system_id,
        "source_id": tuple(ev_charger_session_daily_df["source_id"].tolist())
    },
        as_dataframe=True
    )
        print(sessions_pk_df.head())

        ev_charger_session_daily_df=ev_charger_session_daily_df.merge(sessions_pk_df, on="source_id", how="left")
        print(ev_charger_session_daily_df.head())
        ev_charger_session_daily_df=ev_charger_session_daily_df.rename(columns={"id_y":"session_serial_id"})
        

        ev_charger_session_daily_df = ev_charger_session_daily_df.drop(columns=["source_id","userId","socket_source_id","total_price","extendedBySessionId","originalSessionId",
                                                                                "day_segment_start","day_segment_end","timestamp","energy","power","id_x","supplied_energy_day"])
               
        self.uploader.upsert_session_daily(ev_charger_session_daily_df)

        print("===== ETL COMPLETED SUCCESSFULLY =====")


if __name__ == "__main__":
    SessionsPipeline().run()
