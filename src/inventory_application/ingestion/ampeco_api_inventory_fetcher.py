import pandas as pd
import numpy as np
import requests as req
from logging import Logger
import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.configs.settings import settings

#AMPECO API classifies charging points as chargers and EVSEs as sockets within chargers.
class AMPECO_Inventory_Importer:    

    def __init__(self):
        self.url_charging_points_list= settings.url_charging_points_list
        self.url_charging_points_evses_list=settings.url_charging_points_evses_list
        self.url_locations_list= settings.url_locations_list
        self.api_token = settings.api_token
        self.logger = Logger("ampeco_api_inventory_importer.py")

    #fetch all chargers (charging points) from AMPECO API with pagination
    def fetch_charging_points(self) -> pd.DataFrame:
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}"
        }

        all_charging_points = []
        cursor = None  #first cursor is None
        per_page = 100  #max established by AMPECO API

        try:
            while True:
                params = {"cursor": cursor, "per_page": per_page} if cursor else {"cursor": "null", "per_page": per_page}
                response = req.get(self.url_charging_points_list, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                page_data = data.get("data", [])
                all_charging_points.extend(page_data)

                #get the next cursor from the meta.next_cursor field
                cursor = data.get("meta", {}).get("next_cursor")
                print(f"Fetched {len(page_data)} items, next cursor: {cursor}")

                if not cursor:
                    break  #no more pages

            charging_points_df = pd.DataFrame(all_charging_points)
            print(charging_points_df.head())
            print(f"Total charging points fetched: {len(charging_points_df)}")
            #charging_points_df.to_excel("charging_points.xlsx", index=False)
            #self.logger.info(f"Fetched {len(charging_points_df)} charging points from Ampeco API.")
            return charging_points_df

        except Exception as e:
            print(f"Error fetching charging points: {e}")
            #self.logger.error(f"Error fetching charging points: {e}")
            return pd.DataFrame()
    
    #fetch EVSEs for a given charge point
    def fetch_evse_for_charge_point(self, charge_point_id: int) -> pd.DataFrame:
        url = self.url_charging_points_evses_list.replace("{chargePoint}", str(charge_point_id))
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}"
        }

        try:
            response = req.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            evses_list = data.get("data", [])
            df = pd.DataFrame(evses_list)
            print(f"Charge point {charge_point_id}: {len(df)} EVSEs fetched")
            return df

        except Exception as e:
            print(f"Error fetching EVSEs for charge point {charge_point_id}: {e}")
            return pd.DataFrame()

    #fetch EVSEs for all charge points calling the fetch_evse_for_charge_point method
    def fetch_all_evse(self, charging_points_df: pd.DataFrame) -> pd.DataFrame:
        all_evse = []

        for cp_id in charging_points_df['id']:
            df = self.fetch_evse_for_charge_point(cp_id)
            if not df.empty:
                df["charge_point_id"] = cp_id
                all_evse.append(df)

        if all_evse:
            all_evse_df = pd.concat(all_evse, ignore_index=True)
            #all_evse_df.to_excel("all_evse.xlsx", index=False)
            return all_evse_df
        else:
            return pd.DataFrame()
    
    #fetch all locations from AMPECO API with pagination
    def fetch_locations(self) -> pd.DataFrame:
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}"
        }

        all_locations = []
        cursor = None  
        per_page = 100  

        try:
            while True:
                params = {"cursor": cursor, "per_page": per_page} if cursor else {"cursor": "null", "per_page": per_page}
                response = req.get(self.url_locations_list, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                page_data = data.get("data", [])
                all_locations.extend(page_data)

                
                cursor = data.get("meta", {}).get("next_cursor")
                print(f"Fetched {len(page_data)} items, next cursor: {cursor}")

                if not cursor:
                    break  

            locations_df = pd.DataFrame(all_locations)
            print(locations_df.head())
            print(f"Total locations fetched: {len(locations_df)}")
            # self.logger.info(f"Fetched {len(locations_df)} locations from Ampeco API.")
            
            return locations_df
        
        except Exception as e:
            print(f"Error fetching locations: {e}")
            #self.logger.error(f"Error fetching locations: {e}")
            return pd.DataFrame()       
    
    '''      
    def run(self):
        print("Fetching all charge points...")
        cp_df = self.fetch_charging_points()
        locations_df = self.fetch_locations()

        if not cp_df.empty:
            print("Fetching all EVSEs...")
            evse_df = self.fetch_all_evse(cp_df)
            print(f"Total EVSEs fetched: {len(evse_df)}")
        else:
            print("No charge points found.")
        
        if not locations_df.empty:
            print(f"Total Locations fetched: {len(locations_df)}")
        else:
            print("No locations found.")

  
runner=AMPECO_Inventory_Importer()
runner.run()'''