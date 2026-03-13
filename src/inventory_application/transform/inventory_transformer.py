import pandas as pd

class InventoryTransformer:

    # ----------------------------------------
    # CHARGING POINTS
    # ----------------------------------------
    def clean_charging_points(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        
        drop_cols = [
            "networkType","network","capabilities","security","user","usesRenewableEnergy","chargingZoneId","managedByOperator",
            "monitoringEnabled","autoRecoveryEnabled",
            "usesRenewableEnergy","communicationMode","displayTariffAndCosts","enableAutoFaultRecovery",
            "autoStartWithoutAuthorization","disableAutoStartEmulation","tags","uptimeTrackingEnabled",
            "uptimeTrackingActivatedAt","modelId"
        ]

        df = df.drop(columns=drop_cols, errors="ignore")

        df = df.rename(columns={
            "id": "source_id",
            "name": "charger_id",
            "type": "network",
            "locationId": "local",
            "networkStatus" : "status_connectivity",
            "hardwareStatus" : "status_hardware",
            "firstContactAt" : "start_operation_date"
        })

        df['partner'] = df['partner'].str.get('id').fillna(0).astype('int64')
        
        
        return df

    # ----------------------------------------
    # EVSEs (Sockets)
    # ----------------------------------------
    def clean_evses(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        
        drop_cols = [
           "networkId","allowsReservation","bookingEnabled","roamingOperatorId",
            "createdAt", "lastUpdatedAt","tariffGroupId","label"
        ]

        df = df.drop(columns=drop_cols, errors="ignore")

        df = df.rename(columns={
            "id": "source_id","currentType": "type", "powerOptions":"max_power","charge_point_id":"charge_point_source_id", "physicalReference":"physical_reference","hardwareStatus" : "status_hardware"
        })

        df['max_power'] = df['max_power'].str['maxPower']
        print(df['max_power'].head())

        df['max_power'] = pd.to_numeric(df['max_power'], errors='coerce')/1000  #convert to kW
        df['type'] = df['type'].str.upper()

        return df

    # ----------------------------------------
    # LOCATIONS
    # ----------------------------------------
    def clean_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        drop_cols = [
            "status", "description", "shortDescription", "additionalDescription","streetAddress","postCode",
            "workingHours","roamingOperatorId","roamingOperatorId","tags","lastUpdatedAt","facilities"
        ]

        df = df.drop(columns=drop_cols, errors="ignore")

        df = df.rename(columns={
            "id": "source_id",
            "name": "local"
        })

        #Extract localized name based on country code else take first available
        df['local'] = df.apply(
        lambda row: next(
            (item['translation'] for item in row['local'] if item.get('locale') == row.get('country')),
            row['local'][0]['translation'] if row['local'] else None), axis=1)
        
        df['address'] = df.apply(
        lambda row: next(
            (item['translation'] for item in row['address'] if item.get('locale') == row.get('country')),
            row['address'][0]['translation'] if row['address'] else None), axis=1)
        
        df['latitude'] = df['geoposition'].apply(lambda x: x.get('latitude') if isinstance(x, dict) else None)
        df['longitude'] = df['geoposition'].apply(lambda x: x.get('longitude') if isinstance(x, dict) else None)

        return df
    
    # ----------------------------------------
    # JOIN TABLES
    # ----------------------------------------
    def build_inventory_tables(self, cp_df, evse_df, location_df):
        
        ##

        if cp_df.empty or evse_df.empty:
            print("ERROR: Cannot build inventory table (missing data).")
            return pd.DataFrame()
        
        ##ev_charger

        #data from evse_df to be merged into cp_df
        extracted_evse_df = evse_df[["type", "max_power", "charge_point_source_id"]]
        #count number of sockets per charge point
        extracted_evse_df["no_socket"] = extracted_evse_df.groupby("charge_point_source_id")["charge_point_source_id"].transform("count")
    
        extracted_evse_df = extracted_evse_df.sort_values(by="max_power", ascending=False)
        extracted_evse_df = extracted_evse_df.drop_duplicates(subset=["charge_point_source_id"], keep="first")       


        ev_charger_df=cp_df.merge(extracted_evse_df, left_on="source_id", right_on="charge_point_source_id", how="inner")
        ev_charger_df=ev_charger_df.drop(columns=["charge_point_source_id"])

        #data from location_df to be merged into cp_df
        extracted_location_df = location_df[["source_id", "latitude", "longitude"]]
        extracted_location_df = extracted_location_df.rename(columns={"source_id": "location_source_id"})

        ev_charger_df=ev_charger_df.merge(extracted_location_df, left_on="local", right_on="location_source_id", how="left")
        ev_charger_df=ev_charger_df.drop(columns=["location_source_id"])

        ev_charger_df=ev_charger_df.drop(columns=["lastUpdatedAt","createdAt"], errors="ignore")
        
        ##ev_charger_socket

        extracted_cp_df=cp_df[["source_id","charger_id"]]
        extracted_cp_df=extracted_cp_df.rename(columns={"charger_id": "charger_id_name","source_id": "charge_point_source_id"})
        

        ev_charger_socket_df=evse_df.merge(extracted_cp_df, on="charge_point_source_id", how="inner")
        #ev_charger_socket_df['socket_number']=ev_charger_socket_df.groupby("charge_point_source_id").cumcount()+1
        ev_charger_socket_df['socket_id']=ev_charger_socket_df['charger_id_name'].astype(str) + "_" + ev_charger_socket_df['physical_reference'].astype(str)
        ev_charger_socket_df=ev_charger_socket_df.drop(columns=["charger_id_name"]) #,"socket_number"])
        ev_charger_socket_df['charger_id']=ev_charger_socket_df['charge_point_source_id']


        ##location

        location_df=location_df.drop(columns=["geoposition","latitude","longitude"], errors="ignore")
        

        return ev_charger_df, ev_charger_socket_df, location_df
