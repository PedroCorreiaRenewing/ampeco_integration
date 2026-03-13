import pandas as pd
import numpy as np
import requests as req
from logging import Logger
import sys
sys.path.insert(0, "/root/ampeco_integration/")
from src.configs.settings import settings

#AMPECO API classifies charging points as chargers and EVSEs as sockets within chargers.
class AMPECO_Session_Importer:    

    def __init__(self):
        self.url_sessions_list= settings.url_sessions_list
        self.url_session_consumption=settings.url_session_consumption
        self.api_token = settings.api_token
        self.logger = Logger("ampeco_api_session_fetcher.py")

    #fetch all sessions from AMPECO API with pagination
    def fetch_sessions_paginated(self,last_update_date=None) -> pd.DataFrame:
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}"
        }
            #use function from db_functions to get max date from ev_charger_session table
            #apply logic to fetch sessions with pagination by date
            #if ev_charger_session max_date is not null, fetch sessions from that date - 4 days to current date
            #else fetch sessions from current date - x days to current date

        sessions = []
        cursor = None  #first cursor is None
        per_page = 100  #max established by AMPECO API
        ##FOR TESTING
        page_count=0
        max_pages=20

        try:
            ###FOR TESTING
            #while page_count<max_pages:
            while True:
                params = {
                "per_page": per_page,
                "cursor": "null" if cursor is None else cursor
            }
                if last_update_date is not None:
                    
                    params["filter[lastUpdatedAfter]"] = last_update_date.isoformat()

                response = req.get(
                    self.url_sessions_list,
                    headers=headers,
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                print(data)

                page_data = data.get("data", [])
                if not page_data:
                    break

                yield pd.DataFrame(page_data)

                cursor = data.get("meta", {}).get("next_cursor")

                print(f"Fetched {len(page_data)} sessions | next_cursor={cursor}")

                if not cursor:
                    break  

            sessions_df = pd.DataFrame(sessions)
            print(f"Total sessions fetched: {len(sessions_df)}")

            return sessions_df
        except Exception as e:
            print(f"Error fetching charging points: {e}")
            #self.logger.error(f"Error fetching charging points: {e}")
            return pd.DataFrame()

    #fetch session consumption for a given session
    def fetch_session_consumption(self, sessions_df: pd.DataFrame) -> pd.DataFrame:
        all_session_consumption = [] 
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}"
        }

        #For process optimization, only fetch session consumption for sessions where start_date day != end_date day
        
        sessions_diff_day_df=sessions_df
        sessions_diff_day_df=sessions_diff_day_df[sessions_diff_day_df['stoppedAt'].notnull()]
        
        # Convert columns to datetime objects
        sessions_diff_day_df['startedAt'] = pd.to_datetime(sessions_diff_day_df['startedAt'])
        sessions_diff_day_df['stoppedAt'] = pd.to_datetime(sessions_diff_day_df['stoppedAt'])

        # Now your original line will work
        sessions_diff_day_df = sessions_diff_day_df[(sessions_diff_day_df['startedAt'].dt.day != sessions_diff_day_df['stoppedAt'].dt.day)]
        sessions_diff_day_df = sessions_diff_day_df[['id', 'startedAt', 'stoppedAt']]
        print(f"Total sessions with start_date day different than end_date day: {len(sessions_diff_day_df)}")

        for session_id in sessions_diff_day_df['id']:
            url = self.url_session_consumption.replace("{session}", str(session_id))        

            try:
                response = req.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()

                session_consumption = data.get("data", [])
                df = pd.DataFrame(session_consumption)

                if not df.empty:
                    df['id'] = session_id

                print(f"Session {session_id}: {len(df)} consumption fetched")
                all_session_consumption.append(df)                             

            except Exception as e:
                print(f"Error fetching session {session_id} consumption: {e}")
                return pd.DataFrame()
        if all_session_consumption:
            diff_day_session_consumption_df = pd.concat(all_session_consumption, ignore_index=True)
            
            return diff_day_session_consumption_df
        else:
            return pd.DataFrame()
            
  

'''
runner=AMPECO_Session_Importer()
sessions_df = runner.fetch_sessions()
print(sessions_df.head())
diff_day_session_consumption_df = runner.fetch_session_consumption(sessions_df)
print(diff_day_session_consumption_df.head())'''