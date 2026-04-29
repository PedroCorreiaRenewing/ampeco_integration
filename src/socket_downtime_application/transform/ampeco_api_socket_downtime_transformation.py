import pandas as pd

class SocketDowntimeDataTransformer:

    # ----------------------------------------
    # SOCKET DOWNTIME
    # ----------------------------------------
    def clean_socket_downtime(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        
        print(df.head())

        df = df.rename(columns={
            "id": "source_id",
            "evseId" : "socket_source_id",
            "startedAt" : "start_date",
            "endedAt" : "end_date"
        })      
        
        df = df[df['type'] == 'downtime']

        df=df.drop(columns=[
            "chargePointId","locationId","entryMode","type","statuses"
        ], errors="ignore")  

        df['total_downtime_minutes'] = (pd.to_datetime(df['end_date']) - pd.to_datetime(df['start_date'])).dt.total_seconds() / 60

        return df