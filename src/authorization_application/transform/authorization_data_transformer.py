import pandas as pd

class AuthorizationDataTransformer:

    # ----------------------------------------
    # USERS
    # ----------------------------------------
    def clean_authorization(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        
        if 'roaming' in df.columns:
            roaming_cols = df['roaming'].apply(pd.Series)

            df = pd.concat([df.drop(columns=['roaming']), roaming_cols], axis=1)

            drop_cols = [            
                "createdAt","platformId","platformRoleId","platformRole","sessionIds"
            ]

            df = df.drop(columns=drop_cols, errors="ignore")
            df = df.drop(columns=[c for c in df.columns if str(c) == '0'], errors="ignore")    
        else:
            print("No 'roaming' column found in DataFrame. Skipping roaming expansion.")
            drop_cols = [            
                "createdAt"
            ]

            df = df.drop(columns=drop_cols, errors="ignore")
            df = df.drop(columns=[c for c in df.columns if str(c) == '0'], errors="ignore")   
        

        print(df.head())

        df = df.rename(columns={
            "id": "source_id",
            "userId": "user_id",
            "idTagUid": "card_id",
            "chargePointId": "charger_id",
            "evseId" : "socket_id",
            "lastUpdatedAt" : "last_update_date",
            "rejectionReason" : "rejection_reason",
            "source" : "source_type"
        })              

        if 'charger_id' in df.columns:
            df['charger_id'] = df['charger_id'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else (None if isinstance(x, list) else x))
            df['charger_id'] = (pd.to_numeric(df['charger_id'], errors='coerce').fillna(0).astype(int).astype(str))
            
        if 'socket_id' in df.columns:
            df['socket_id'] = df['socket_id'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else (None if isinstance(x, list) else x))
            df['socket_id'] = (pd.to_numeric(df['socket_id'], errors='coerce').fillna(0).astype(int).astype(str))
        
        return df

  