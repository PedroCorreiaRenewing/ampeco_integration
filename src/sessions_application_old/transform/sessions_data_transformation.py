import pandas as pd

class SessionsTransformer:

    # ----------------------------------------
    # SESSIONS
    # ----------------------------------------
    def clean_session_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        
        drop_cols = [
            "chargePointId","connectorId","reason","powerKw","totalAmount","tax","currency","nonBillableEnergy",
            "paymentType","paymentMethodId","terminalId","paymentStatus","authorizationId","idTagLabel",
            "extendingSessionId","reimbursementEligibility","tariffSnapshotId","electricityCost","externalSessionId",
            "evsePhysicalReference","paymentStatusUpdatedAt","receiptId","energyConsumption","billingStatus","power",
            "lastUpdatedAt","socPercent"
        ]

        df = df.drop(columns=drop_cols, errors="ignore")

        df = df.rename(columns={
            "id": "source_id",
            "evseId": "socket_source_id",
            "startedAt": "start_date",
            "stoppedAt": "end_date",
            "energy" : "total_energy_kwh",
            "amount" : "total_price",
            "idTag": "card_id"
        })

        
        return df
   
  
    def build_inventory_tables(self, session_df,diff_day_session_consumption_df):
        
        ##

        if session_df.empty:
            print("ERROR: Cannot build session table (missing data).")
            return pd.DataFrame()
        
        ev_charger_session_df=session_df
        ev_charger_session_df['total_energy_kwh']=ev_charger_session_df['total_energy_kwh'].astype(float)/1000.0
        ev_charger_session_df['total_duration_min']=((pd.to_datetime(ev_charger_session_df['end_date']) - pd.to_datetime(ev_charger_session_df['start_date'])).dt.total_seconds())/60.0
        ev_charger_session_df['session_id']=ev_charger_session_df['source_id']

        ##Processing diff day session consumption to get supplied_energy_day per sub_session_id
        
        # 1. Preparação: Converter datas e ordenar
        diff_day_session_consumption_df['timestamp'] = pd.to_datetime(diff_day_session_consumption_df['timestamp'], errors='coerce')
        diff_day_session_consumption_df = diff_day_session_consumption_df.dropna(subset=['timestamp']).sort_values(['id', 'timestamp'])

        # 2. Pegar o último registo de cada dia
        last_record_per_day = diff_day_session_consumption_df.groupby(['id', diff_day_session_consumption_df['timestamp'].dt.date]).tail(1).copy()

        # 3. Cálculo da energia do dia (Subtração em cascata)
        # Criamos o 'prev_energy' (valor do dia anterior)
        last_record_per_day['prev_energy'] = last_record_per_day.groupby('id')['energy'].shift(1)

        # Se prev_energy for nulo (primeiro dia), a energia do dia é o próprio valor de 'energy'
        # Se não for nulo, é a diferença (hoje - ontem)
        last_record_per_day['supplied_energy_day'] = last_record_per_day['energy'] - last_record_per_day['prev_energy'].fillna(0)

        # 4. Criar sub_session_id (ex: 502_01, 502_02)
        last_record_per_day['counter'] = last_record_per_day.groupby('id').cumcount() + 1
        last_record_per_day['sub_session_id'] = last_record_per_day['id'].astype(str) + "_" + last_record_per_day['counter'].map('{:02d}'.format)

        # 5. Limpeza para o Excel
        last_record_per_day['timestamp'] = last_record_per_day['timestamp'].dt.tz_localize(None)
        energy_per_sub_session_df = last_record_per_day.drop(columns=['prev_energy', 'counter'])

        energy_per_sub_session_df.to_excel("resultado_final.xlsx", index=False)

        ############### EV CHARGER SESSION DAILY ##################

        ev_charger_session_df['start_date'] = pd.to_datetime(ev_charger_session_df['start_date']).dt.tz_localize(None)
        ev_charger_session_df['end_date'] = pd.to_datetime(ev_charger_session_df['end_date']).dt.tz_localize(None)
        ev_charger_session_df=ev_charger_session_df.dropna(subset=['start_date', 'end_date'])

        rows = []

        for _, row in ev_charger_session_df.iterrows():
            # Define start and end date
            s_start = row['start_date']
            s_end = row['end_date']
            
            # Generate list of days between start and end date.
            
            date_range = pd.date_range(start=s_start.date(), end=s_end.date())

            for i, current_day in enumerate(date_range):
                
                day_start = max(pd.Timestamp(current_day), s_start)
                
                
                next_day = pd.Timestamp(current_day) + pd.Timedelta(days=1)
                day_end = min(next_day, s_end)
                
                #Calculate day duration in minutes
                day_duration = (day_end - day_start).total_seconds() / 60                
               
                row_data = row.copy()
                row_data['id_day'] = current_day.strftime('%Y-%m-%d')
                row_data['sub_session_id'] = f"{row['session_id']}_{i+1:02}"
                row_data['total_duration_min'] = round(day_duration, 2)
                
                row_data['day_segment_start'] = day_start
                row_data['day_segment_end'] = day_end
                
                rows.append(row_data)

        ev_charger_session_daily_df = pd.DataFrame(rows)

        
        cols_to_drop = ['start_date', 'end_date', 'total_energy_kwh', 'socket_id', 'emsp', 'card_id']
        # Use errors = 'ignore' to avoid error if column doesn't exist.
        ev_charger_session_daily_df = ev_charger_session_daily_df.drop(columns=cols_to_drop, errors='ignore')
        #ev_charger_session_daily_df.to_excel("ev_charger_session_daily.xlsx", index=False)

        ev_charger_session_daily_df = ev_charger_session_daily_df.merge(energy_per_sub_session_df, on='sub_session_id', how='left')

        ev_charger_session_daily_df['total_energy_kwh'] = ev_charger_session_daily_df.apply(
            lambda row: row['supplied_energy_day'] if row['supplied_energy_day'] is not None
            else 0 if '_01' not in row['sub_session_id']
            else row['total_energy_kwh'],
            axis=1
        )

        single_row_sessions = ev_charger_session_daily_df[ev_charger_session_daily_df.groupby('session_id')['session_id'].transform('count') == 1].copy()
        single_row_sessions = single_row_sessions[single_row_sessions['total_energy_kwh'].isnull()]
        
        single_row_sessions['total_energy_kwh'] = single_row_sessions.apply(
            lambda row: next((x for x in ev_charger_session_df.loc[ev_charger_session_df['session_id'] == row['session_id']]['total_energy_kwh'] if x is not None), None),
            axis=1
        )
        
        ev_charger_session_daily_df.update(single_row_sessions)

        #ev_charger_session_daily_df.to_excel("ev_charger_session_daily_total.xlsx", index=False)

        
        
        

        return ev_charger_session_df, ev_charger_session_daily_df
