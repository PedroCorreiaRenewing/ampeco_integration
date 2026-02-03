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

    def upsert_inventory_user(self,user):
        self.interactor.upsert_dataframe("users", user, conflict_columns=["source_id", "source_system_id"])

    def upsert_inventory_user_group_user(self,user_group_user):
        self.interactor.upsert_dataframe("user_group_user", user_group_user, conflict_columns=["user_id", "user_group_id"])

    def upsert_inventory_user_groups(self, user_groups):
        self.interactor.upsert_dataframe("user_group", user_groups, conflict_columns=["source_id", "source_system_id"])