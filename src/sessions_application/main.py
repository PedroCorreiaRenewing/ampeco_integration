import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.sessions_application.ingestion.ampeco_api_session_fetcher import AMPECO_Session_Importer
from src.sessions_application.transform.sessions_data_transformation import SessionsTransformer
from src.sessions_application.load.db_uploader import DBUploader
from src.repository.db_functions import PostgresInteraction    
from src.configs.settings import settings
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


class SessionsPipeline:

    def __init__(self):
        self.fetcher = AMPECO_Session_Importer()
        self.transformer = SessionsTransformer()
        self.uploader = DBUploader(settings.database_url)
        engine = create_engine(settings.database_url)
        self.db_interactor=PostgresInteraction(engine)

    def run(self):
        
       

        print("===== START AMPECO SESSIONS ETL =====")

        max_date = self.db_interactor.get_max_value(
            column_name="last_update_date",
            table_name="ev_charger_session"
        )

        fetch_from_date = (
            max_date - pd.Timedelta(days=4)
            if pd.notnull(max_date)
            else pd.Timestamp("2022-01-01")
        )

        print(f"Fetching sessions from: {fetch_from_date}")

        source_system_id = self.uploader.get_source_system_id("AMPECO")

        for i, session_df in enumerate(
            self.fetcher.fetch_sessions_paginated(fetch_from_date), start=1
        ):
            print(f"Processing batch {i} with {len(session_df)} sessions")

            if session_df.empty:
                print("Batch vazio — skipping consumption fetch")
                continue

            # -------- FETCH CONSUMPTION --------
            diff_day_session_consumption_df = (
                self.fetcher.fetch_session_consumption(session_df)
            )

            # -------- TRANSFORM --------
            session_clean = self.transformer.clean_session_data(session_df)

            ev_charger_session_df, ev_charger_session_daily_df = (
                self.transformer.build_inventory_tables(
                    session_clean,
                    diff_day_session_consumption_df
                )
            )

            ev_charger_session_df["source_system_id"] = source_system_id
            ev_charger_session_df = ev_charger_session_df.where(
                pd.notnull(ev_charger_session_df), None
            )

            socket_pk_df = self.db_interactor.fetch(
                table_name="ev_charger_socket",
                columns=["id", "source_id"],
                where={
                    "source_system_id": source_system_id,
                    "source_id": tuple(ev_charger_session_df["socket_source_id"].astype(str).tolist())
                },
                as_dataframe=True
            )

            socket_pk_df=socket_pk_df.rename(columns={"id": "socket_id", "source_id": "socket_source_match_id"})

            ev_charger_session_df["socket_source_id"] = ev_charger_session_df["socket_source_id"].astype("Int64")
            socket_pk_df["socket_source_match_id"] = socket_pk_df["socket_source_match_id"].astype("Int64")

            ev_charger_session_df = ev_charger_session_df.merge(
                socket_pk_df, left_on="socket_source_id", right_on="socket_source_match_id", how="left"
            )            

            ev_charger_session_df['socket_id'] = ev_charger_session_df['socket_id'].replace({np.nan: None})
            ev_charger_session_df['user_id'] = ev_charger_session_df['user_id'].replace({np.nan: None})

            session_status_pk_df = self.db_interactor.fetch(
                table_name="session_status",
                columns=["id", "source_status"],
                where=
                    {"source_entity": source_system_id},
                as_dataframe=True
            )

            session_status_pk_df['id'] = session_status_pk_df['id'].astype("Int64")
            session_status_pk_df.rename(columns={"id": "status_new"}, inplace=True)

            session_status_pk_df['source_status'] = session_status_pk_df['source_status'].str.lower()
            ev_charger_session_df['status'] = ev_charger_session_df['status'].str.lower()

            ev_charger_session_df = ev_charger_session_df.merge(
                session_status_pk_df, left_on="status", right_on="source_status", how="left"
            )
            ev_charger_session_df.drop(columns=["status","source_status"], inplace=True)
            ev_charger_session_df = ev_charger_session_df.rename(columns={"status_new": "status"})

            ev_charger_session_df['authorization_id'] = ev_charger_session_df['authorization_id'].replace({np.nan: None})
            
            authorizations_pk_df = self.db_interactor.fetch(
                table_name="authorizations_history",
                columns=["id", "source_id"],
                where={
                    "source_system_id": source_system_id,
                    "source_id": tuple(ev_charger_session_df["authorization_id"].astype(str).tolist())
                },
                as_dataframe=True
            )

            authorizations_pk_df['source_id'] = authorizations_pk_df['source_id'].astype("Int64")
            authorizations_pk_df.rename(columns={"id": "authorization_new", "source_id":"authorization_source_id"}, inplace=True)

            ev_charger_session_df['authorization_id'] = ev_charger_session_df['authorization_id'].astype("Int64")

            ev_charger_session_df = ev_charger_session_df.merge(
                authorizations_pk_df, left_on="authorization_id", right_on="authorization_source_id", how="left"
            )
            ev_charger_session_df.drop(columns=["authorization_id","source_id"], inplace=True)
            ev_charger_session_df = ev_charger_session_df.rename(columns={"authorization_new": "authorization_id"})

            ev_charger_session_df["authorization_id"] = ev_charger_session_df["authorization_id"].replace({np.nan: None})

            # -------- LOAD (SESSION) --------
            print("Upserting ev_charger_session...")
            self.uploader.upsert_session(ev_charger_session_df)

            # -------- LOAD (DAILY) --------
            sessions_pk_df = self.db_interactor.fetch(
                table_name="ev_charger_session",
                columns=["id", "source_id"],
                where={
                    "source_system_id": source_system_id,
                    "source_id": tuple(ev_charger_session_daily_df["source_id"].tolist())
                },
                as_dataframe=True
            )

            ev_charger_session_daily_df = (
                ev_charger_session_daily_df
                .merge(sessions_pk_df, on="source_id", how="left")
                .rename(columns={"id": "session_serial_id"})
            )

            ev_charger_session_daily_df = ev_charger_session_daily_df.drop(
                columns=[
                    "source_id", "userId", "socket_source_id", "total_price",
                    "extendedBySessionId", "originalSessionId",
                    "day_segment_start", "day_segment_end",
                    "timestamp", "energy", "power", "supplied_energy_day"
                ],
                errors="ignore"
            )

            print("Upserting ev_charger_session_daily...")
            self.uploader.upsert_session_daily(ev_charger_session_daily_df)

            print(f"Batch {i} completed ✅")

        print("===== ETL COMPLETED SUCCESSFULLY =====")



if __name__ == "__main__":
    SessionsPipeline().run()
