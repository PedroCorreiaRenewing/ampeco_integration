import pandas as pd

class UsersDataTransformer:

    # ----------------------------------------
    # USERS
    # ----------------------------------------
    def clean_users(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        
        drop_cols = [
            "requirePasswordReset","middleName","postCode","address","vehicleNo","personal_id","skipPreAuthorization",
            "subscriptionId","externalId","lastUpdatedAt","createdBy","options","termsAndPoliciesIdsWithConsent"
        ]

        df = df.drop(columns=drop_cols, errors="ignore")

        df = df.rename(columns={
            "id": "source_id",
            "emailVerified": "email_verified",
            "firstName": "first_name",
            "lastName": "last_name",
            "receiveNewsAndPromotions" : "receive_news_and_promotions",
            "createdAt" : "created_at"
        })

        df_user_group_user=df[['source_id','userGroupIds']].explode('userGroupIds')

        df =df.drop(columns=['userGroupIds'])
        
        df["email_verified"] = (df["email_verified"]
    .replace("", pd.NA)
    .astype("boolean"))

        
        return df, df_user_group_user

    # ----------------------------------------
    # User Groups
    # ----------------------------------------
    def clean_user_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        
        drop_cols = [
            "partnerId","description","lastUpdatedAt"
        ]

        df = df.drop(columns=drop_cols, errors="ignore")

        df = df.rename(columns={
            "id": "source_id"
        })        

        return df

    ''' 
    def build_users_data_tables(self, users_df, user_group_user_df, user_groups_df):
        
        ##

        if users_df.empty or user_group_user_df.empty or user_groups_df.empty:
            print("ERROR: Cannot build user data tables (missing data).")
            return pd.DataFrame()
        
        ##STOPED HERE##
        
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

        ev_charger_df=ev_charger_df.drop(columns=["status","status_connectivity","hardwareStatus","lastUpdatedAt","createdAt"], errors="ignore")
        
        ##ev_charger_socket

        extracted_cp_df=cp_df[["source_id","charger_id"]]
        extracted_cp_df=extracted_cp_df.rename(columns={"charger_id": "charger_id_name","source_id": "charge_point_source_id"})
        

        ev_charger_socket_df=evse_df.merge(extracted_cp_df, on="charge_point_source_id", how="inner")
        ev_charger_socket_df['socket_number']=ev_charger_socket_df.groupby("charge_point_source_id").cumcount()+1
        ev_charger_socket_df['socket_id']=ev_charger_socket_df['charger_id_name'].astype(str) + "_" + ev_charger_socket_df['socket_number'].astype(str)
        ev_charger_socket_df=ev_charger_socket_df.drop(columns=["charger_id_name","socket_number"])
        ev_charger_socket_df['charger_id']=ev_charger_socket_df['charge_point_source_id']


        ##location

        location_df=location_df.drop(columns=["geoposition","latitude","longitude"], errors="ignore")
        

        return ev_charger_df, ev_charger_socket_df, location_df'''
