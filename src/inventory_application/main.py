import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.inventory_application.ingestion.ampeco_api_inventory_fetcher import AMPECO_Inventory_Importer
from src.inventory_application.transform.inventory_transformer import InventoryTransformer
from src.inventory_application.load.db_uploader import DBUploader
from src.repository.db_functions import PostgresInteraction    
from src.configs.settings import settings
from sqlalchemy import create_engine
import pandas as pd


class InventoryPipeline:

    def __init__(self):
        self.fetcher = AMPECO_Inventory_Importer()
        self.transformer = InventoryTransformer()
        self.uploader = DBUploader(settings.database_url)
        engine = create_engine(settings.database_url)
        self.db_interactor=PostgresInteraction(engine)

    def run(self):
        
        print("===== START AMPECO INVENTORY ETL =====")

        
        # 1. FETCH
                
        print("Fetching charging points...")
        cp_df = self.fetcher.fetch_charging_points()

        print("Fetching locations...")
        loc_df = self.fetcher.fetch_locations()

        print("Fetching EVSEs...")
        evse_df = self.fetcher.fetch_all_evse(cp_df)

        
        # 2. TRANSFORM AND LOAD
        
        cp_clean = self.transformer.clean_charging_points(cp_df)
        evse_clean = self.transformer.clean_evses(evse_df)
        locations_clean = self.transformer.clean_locations(loc_df)   
        
        source_system_id = self.uploader.get_source_system_id("AMPECO")

        ev_charger_df,ev_charger_socket_df, locations_df=self.transformer.build_inventory_tables(cp_clean, evse_clean, locations_clean)
        ev_charger_df['source_system_id'] = source_system_id
        ev_charger_socket_df['source_system_id'] = source_system_id
        locations_df['source_system_id'] = source_system_id 
        ev_charger_df=ev_charger_df.sort_values(by="source_id", ascending=True)
        ev_charger_socket_df=ev_charger_socket_df.sort_values(by="source_id", ascending=True)
        locations_df=locations_df.sort_values(by="source_id", ascending=True)   

        locations_df.to_excel("locations.xlsx", index=False)
        
        ev_charger_socket_df.to_excel("ev_charger_socket.xlsx", index=False)
        #Insert Locations into DB
        print("Uploading local to database...")
        self.uploader.upsert_inventory_local(locations_df) 
        
        #locations pk
        locations_pk_df = self.db_interactor.fetch(
        table_name="locations",
        columns=["id", "source_id"],
        as_dataframe=True
    )
        locations_pk_df["source_id"] = (
    pd.to_numeric(locations_pk_df["source_id"], errors="coerce")
    .astype("Int64"))
        
        print(locations_pk_df.head())
        ev_charger_df = ev_charger_df.merge(locations_pk_df, left_on="local", right_on="source_id", how="left")
        ev_charger_df.drop(columns=["local","source_id_y"], inplace=True)
        ev_charger_df = ev_charger_df.rename(columns={"id": "local", "source_id_x": "source_id"})

        ev_charger_df.to_excel("ev_charger.xlsx", index=False)

        #charger status pk
        charger_status_pk_df = self.db_interactor.fetch(
        table_name="charger_status",
        columns=["id",  "source_status"],
        where={"source_entity": source_system_id},
        as_dataframe=True
    )        
        charger_status_pk_df['id'] = charger_status_pk_df['id'].astype("Int64")
        charger_status_pk_df.rename(columns={"id": "status_new"}, inplace=True)
        
        
        charger_connection_status_pk_df = self.db_interactor.fetch(
        table_name="charger_connection_status",
        columns=["id",  "source_status"],
        where={"source_entity": source_system_id},
        as_dataframe=True
    )
        charger_connection_status_pk_df['id'] = charger_connection_status_pk_df['id'].astype("Int64")
        charger_connection_status_pk_df.rename(columns={"id": "status_connectivity_new"}, inplace=True)
        
        
        charger_hardware_status_pk_df = self.db_interactor.fetch(
        table_name="charger_hardware_status",
        columns=["id",  "source_status"],
        where={"source_entity": source_system_id},
        as_dataframe=True
    )
        charger_hardware_status_pk_df['id'] = charger_hardware_status_pk_df['id'].astype("Int64")
        charger_hardware_status_pk_df.rename(columns={"id": "status_hardware_new"}, inplace=True)
        

        charger_status_pk_df['source_status'] = charger_status_pk_df['source_status'].str.lower()
        charger_connection_status_pk_df['source_status'] = charger_connection_status_pk_df['source_status'].str.lower()
        charger_hardware_status_pk_df['source_status'] = charger_hardware_status_pk_df['source_status'].str.lower()
        ev_charger_df['status'] = ev_charger_df['status'].str.lower()
        ev_charger_df['status_connectivity'] = ev_charger_df['status_connectivity'].str.lower()
        ev_charger_df['status_hardware'] = ev_charger_df['status_hardware'].str.lower()

        ev_charger_df = ev_charger_df.merge(charger_status_pk_df, left_on="status", right_on="source_status", how="left")
        ev_charger_df.drop(columns=["status","source_status"], inplace=True)
        ev_charger_df = ev_charger_df.rename(columns={"status_new": "status"})

        ev_charger_df = ev_charger_df.merge(charger_connection_status_pk_df, left_on="status_connectivity", right_on="source_status", how="left")
        ev_charger_df.drop(columns=["status_connectivity","source_status"], inplace=True)
        ev_charger_df = ev_charger_df.rename(columns={"status_connectivity_new": "status_connectivity"})

        ev_charger_df = ev_charger_df.merge(charger_hardware_status_pk_df, left_on="status_hardware", right_on="source_status", how="left")
        ev_charger_df.drop(columns=["status_hardware","source_status"], inplace=True)
        ev_charger_df = ev_charger_df.rename(columns={"status_hardware_new": "status_hardware"})      
       

        #partners pk
        partners_pk_df = self.db_interactor.fetch(
        table_name="partners",
        columns=["id", "source_id"],
        as_dataframe=True
    )
        partners_pk_df["source_id"] = (
    pd.to_numeric(partners_pk_df["source_id"], errors="coerce")
    .astype("Int64"))
        
        print(partners_pk_df.head())
        ev_charger_df = ev_charger_df.merge(partners_pk_df, left_on="partner", right_on="source_id", how="left")
        ev_charger_df.drop(columns=["partner","source_id_y"], inplace=True)
        ev_charger_df = ev_charger_df.rename(columns={"id": "partner", "source_id_x": "source_id"})
        ev_charger_df["partner"] = ev_charger_df["partner"].astype("Int64")
        print(ev_charger_df.head())

        ev_charger_df.to_excel("ev_charger.xlsx", index=False)
        #Insert ev_charger into DB
        print("Uploading ev_charger to database...")
        self.uploader.upsert_inventory_ev_charger(ev_charger_df) 

        ev_charger_pk_df = self.db_interactor.fetch(
        table_name="ev_charger",
        columns=["id", "source_id"],
        as_dataframe=True
    )
        ev_charger_pk_df["source_id"] = (
    pd.to_numeric(ev_charger_pk_df["source_id"], errors="coerce")
    .astype("Int64"))

        print(ev_charger_pk_df.head())
        ev_charger_socket_df = ev_charger_socket_df.merge(ev_charger_pk_df,left_on="charger_id", right_on="source_id", how="left")
        ev_charger_socket_df.drop(columns=["charger_id","source_id_y"], inplace=True)
        ev_charger_socket_df = ev_charger_socket_df.rename(columns={"id": "charger_id","source_id_x": "source_id"})
        print(ev_charger_socket_df.head())

        #socket status pk
        socket_status_pk_df = self.db_interactor.fetch(
        table_name="charger_socket_status",
        columns=["id",  "source_status"],
        where={"source_entity": source_system_id},
        as_dataframe=True
    )
        
        socket_status_pk_df['id'] = socket_status_pk_df['id'].astype("Int64")
        socket_status_pk_df.rename(columns={"id": "status_new"}, inplace=True)
        
        socket_hardware_status_pk_df = self.db_interactor.fetch(
        table_name="charger_socket_hardware_status",
        columns=["id",  "source_status"],
        where={"source_entity": source_system_id},
        as_dataframe=True
    )
        socket_hardware_status_pk_df['id'] = socket_hardware_status_pk_df['id'].astype("Int64")
        socket_hardware_status_pk_df = socket_hardware_status_pk_df.rename(columns={"id": "status_hardware_new"})
        
        
        socket_status_pk_df['source_status'] = socket_status_pk_df['source_status'].str.lower()
        socket_hardware_status_pk_df['source_status'] = socket_hardware_status_pk_df['source_status'].str.lower()
        ev_charger_socket_df['status'] = ev_charger_socket_df['status'].str.lower()
        ev_charger_socket_df['status_hardware'] = ev_charger_socket_df['status_hardware'].str.lower()
        
        ev_charger_socket_df = ev_charger_socket_df.merge(socket_status_pk_df, left_on="status", right_on="source_status", how="left")
        ev_charger_socket_df.drop(columns=["status","source_status"], inplace=True)
        ev_charger_socket_df = ev_charger_socket_df.rename(columns={"status_new": "status"})
        

        ev_charger_socket_df = ev_charger_socket_df.merge(socket_hardware_status_pk_df, left_on="status_hardware", right_on="source_status", how="left")
        ev_charger_socket_df.drop(columns=["status_hardware","source_status"], inplace=True)
        ev_charger_socket_df = ev_charger_socket_df.rename(columns={"status_hardware_new": "status_hardware"})

        duplicates = ev_charger_socket_df.duplicated(subset=["socket_id", "source_system_id"], keep=False)
        print(ev_charger_socket_df[duplicates])
        
        ev_charger_socket_df.to_excel("ev_charger_socket.xlsx", index=False)
        #Insert ev_charger_socket into DB
        print("Uploading ev_charger_socket to database...")
        self.uploader.upsert_inventory_ev_charger_socket(ev_charger_socket_df)


        '''
        ev_charger_df.to_excel("final_ev_charger.xlsx", index=False)
        ev_charger_socket_df.to_excel("final_ev_charger_socket.xlsx", index=False)
        locations_df.to_excel("final_locations.xlsx", index=False)  
        
        #TEST
        import pandas as pd
        ev_charger_df=pd.read_excel("final_ev_charger.xlsx")
        ev_charger_socket_df=pd.read_excel("final_ev_charger_socket.xlsx")
        locations_df=pd.read_excel("final_locations.xlsx")
        ev_charger_df=ev_charger_df.sort_values(by="source_id", ascending=True)
        ev_charger_socket_df=ev_charger_socket_df.sort_values(by="source_id", ascending=True)
        locations_df=locations_df.sort_values(by="source_id", ascending=True)
        '''             
               

        print("===== ETL COMPLETED SUCCESSFULLY =====")


if __name__ == "__main__":
    InventoryPipeline().run()
