import pandas as pd

class PartnersTransformer:

    # ----------------------------------------
    # USERS
    # ----------------------------------------
    def clean_partners(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df       
        
        drop_cols = [
            "regNo","postcode","faultNotificationsEmail","lastUpdatedAt","monthlyPlatformFee","options","corporateBilling","externalId","operatorId"
        ]

        df = df.drop(columns=drop_cols, errors="ignore")
        print(df.head())

        df = df.rename(columns={
            "id": "source_id",
            "vatNo": "vat_number",
            "contactPerson": "contact_person"
        })              
        
        return df

  