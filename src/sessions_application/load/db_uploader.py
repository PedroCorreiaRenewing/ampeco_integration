from sqlalchemy import create_engine, text
from src.repository.db_functions import PostgresInteraction    

class DBUploader:

    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        self.interactor= PostgresInteraction.from_engine(self.engine)

    def get_source_system_id(self, source_name: str) -> int:
        query = text("""
            SELECT *
            FROM source_system
            WHERE source_name = :source_name
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"source_name": source_name}).fetchone()

        if not result:
            raise ValueError(f"Source system '{source_name}' not found in DB")

        return result[0]

    def upsert_session(self,ev_charger_session):
        self.interactor.upsert_dataframe("ev_charger_session", ev_charger_session, conflict_columns=["source_id", "source_system_id"])

    def upsert_session_daily(self,ev_charger_session_daily):
        self.interactor.upsert_dataframe("ev_charger_session_daily", ev_charger_session_daily, conflict_columns=["sub_session_id","session_serial_id"])