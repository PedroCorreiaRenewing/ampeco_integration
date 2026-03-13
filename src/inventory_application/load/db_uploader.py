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

    def upsert_inventory_local(self,locations):
        self.interactor.upsert_dataframe("locations", locations, conflict_columns=["source_id", "source_system_id"])

    def upsert_inventory_ev_charger(self,ev_charger ):
        self.interactor.upsert_dataframe("ev_charger", ev_charger, conflict_columns=["charger_id", "source_system_id"])

    def upsert_inventory_ev_charger_socket(self, ev_charger_socket):
        self.interactor.upsert_dataframe("ev_charger_socket", ev_charger_socket, conflict_columns=["socket_id", "source_system_id"])