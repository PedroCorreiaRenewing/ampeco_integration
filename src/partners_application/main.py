import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.partners_application.ingestion.ampeco_api_partner_data_fetcher import AMPECO_Partners_Data_Importer
from src.partners_application.transform.partner_data_transformer import PartnersTransformer
from src.partners_application.load.db_uploader import DBUploader
from src.repository.db_functions import PostgresInteraction    
from src.configs.settings import settings
from sqlalchemy import create_engine
import pandas as pd


class PartnersPipeline:

    def __init__(self):
        self.fetcher = AMPECO_Partners_Data_Importer()
        self.transformer = PartnersTransformer()
        self.uploader = DBUploader(settings.database_url)
        engine = create_engine(settings.database_url)
        self.db_interactor=PostgresInteraction(engine)

    def run(self):
        
        print("===== START AMPECO INVENTORY ETL =====")

        
        # 1. FETCH
                
        print("Fetching partners...")
        partner_df = self.fetcher.fetch_partners()       

        
        # 2. TRANSFORM AND LOAD
        
        partner_clean = self.transformer.clean_partners(partner_df)
        
        source_system_id = self.uploader.get_source_system_id("AMPECO")

        partner_clean["source_system_id"] = source_system_id
        
        #Insert Partners into DB
        print("Uploading partners to database...")
        self.uploader.upsert_inventory_partner(partner_clean) 

        print("===== ETL COMPLETED SUCCESSFULLY =====")


if __name__ == "__main__":
    PartnersPipeline().run()
