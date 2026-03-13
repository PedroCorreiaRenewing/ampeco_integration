import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.users_application.ingestion.ampeco_api_users_data_fetcher import AMPECO_Users_Data_Importer
from src.users_application.transform.users_data_transformer import UsersDataTransformer
from src.users_application.load.db_uploader import DBUploader
from src.repository.db_functions import PostgresInteraction    
from src.configs.settings import settings
from sqlalchemy import create_engine
import pandas as pd


class UsersPipeline:

    def __init__(self):
        self.fetcher = AMPECO_Users_Data_Importer()
        self.transformer = UsersDataTransformer()
        self.uploader = DBUploader(settings.database_url)
        engine = create_engine(settings.database_url)
        self.db_interactor=PostgresInteraction(engine)

    def run(self):
        
        print("===== START AMPECO USERS ETL =====")

        
        # 1. FETCH
                
        print("Fetching users...")
        users_df = self.fetcher.fetch_users()

        print("Fetching user groups...")
        user_groups_df = self.fetcher.fetch_user_groups()       

        
        # 2. TRANSFORM
        
        users_df, user_group_user_df = self.transformer.clean_users(users_df)
        user_groups_df = self.transformer.clean_user_groups(user_groups_df)  

        print(users_df.head())
        print(user_group_user_df.head()) 
        print(user_groups_df.head())
        
        
        source_system_id = self.uploader.get_source_system_id("AMPECO")

        ##STOPED HERE##
        
        users_df['source_system_id'] = source_system_id
        user_group_user_df['source_system_id'] = source_system_id
        user_groups_df['source_system_id'] = source_system_id 
        users_df=users_df.sort_values(by="source_id", ascending=True)
        user_group_user_df=user_group_user_df.sort_values(by="source_id", ascending=True)
        user_groups_df=user_groups_df.sort_values(by="source_id", ascending=True)       
        users_df.to_excel("users_df.xlsx", index=False)
        
        #user status pk
        user_status_pk_df = self.db_interactor.fetch(
        table_name="user_status",
        columns=["id",  "source_status"],
        where={"source_entity": source_system_id},
        as_dataframe=True
    )
        user_status_pk_df['id'] = user_status_pk_df['id'].astype("Int64")
        user_status_pk_df.rename(columns={"id": "status_new"}, inplace=True)
        
        user_status_pk_df['source_status'] = user_status_pk_df['source_status'].str.lower()
        users_df['status'] = users_df['status'].str.lower()
        users_df = users_df.merge(user_status_pk_df, left_on="status", right_on="source_status", how="left")
        users_df.drop(columns=["status", "source_status"], inplace=True)
        users_df = users_df.rename(columns={"status_new": "status"})

        print("Uploading users to database...")
        self.uploader.upsert_inventory_user(users_df)
        print("Uploading user groups to database...")
        self.uploader.upsert_inventory_user_groups(user_groups_df)  

        user_pk_df = self.db_interactor.fetch(
        table_name="users",
        columns=["id", "source_id"],
        as_dataframe=True
    )
        user_group_user_df["source_id"] = (
    pd.to_numeric(user_pk_df["source_id"], errors="coerce")
    .astype("Int64"))
        user_pk_df["source_id"] = (
    pd.to_numeric(user_pk_df["source_id"], errors="coerce")
    .astype("Int64"))

        print(user_pk_df.head())
        user_group_user_df = user_group_user_df.merge(user_pk_df, on="source_id", how="left")
        user_group_user_df.drop(columns=["source_id"], inplace=True)
        user_group_user_df = user_group_user_df.rename(columns={"id": "user_id"})
        print(user_group_user_df.head())

        user_group_pk_df = self.db_interactor.fetch(
        table_name="user_group",
        columns=["id", "source_id"],
        as_dataframe=True
    )
        user_group_user_df["userGroupIds"] = (
    pd.to_numeric(user_group_user_df["userGroupIds"], errors="coerce")
    .astype("Int64"))
        user_group_pk_df["source_id"] = (
    pd.to_numeric(user_group_pk_df["source_id"], errors="coerce")
    .astype("Int64"))

        user_group_user_df = user_group_user_df.merge(user_group_pk_df, left_on="userGroupIds", right_on="source_id", how="left")
        user_group_user_df.drop(columns=["userGroupIds","source_id"], inplace=True)
        user_group_user_df = user_group_user_df.rename(columns={"id": "user_group_id"})
        user_group_user_df.dropna(subset=["user_id","user_group_id"], inplace=True)

        print("Uploading user group users to database...")
        self.uploader.upsert_inventory_user_group_user(user_group_user_df)              
             

        print("===== ETL COMPLETED SUCCESSFULLY =====")


if __name__ == "__main__":
    UsersPipeline().run()
