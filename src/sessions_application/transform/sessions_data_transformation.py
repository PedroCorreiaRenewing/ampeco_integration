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
            "paymentType","paymentMethodId","terminalId","paymentStatus","idTagLabel",
            "extendingSessionId","reimbursementEligibility","tariffSnapshotId","electricityCost","externalSessionId",
            "evsePhysicalReference","paymentStatusUpdatedAt","receiptId","energyConsumption","billingStatus","power",
            "socPercent","idTag"
        ]

        df = df.drop(columns=drop_cols, errors="ignore")

        df = df.rename(columns={
            "id": "source_id",
            "evseId": "socket_source_id",
            "startedAt": "start_date",
            "stoppedAt": "end_date",
            "energy" : "total_energy_kwh",
            "amount" : "total_price",
            "authorizationId" : "authorization_id",
            #"idTag": "card_id",
            "lastUpdatedAt" : "last_update_date",
            "userId": "user_id"
        })
        print(df)
        
        return df
   
  
    def build_inventory_tables(self, session_df,diff_day_session_consumption_df):
        
        ##

        if session_df.empty:
            print("ERROR: Cannot build session table (missing data).")
            return pd.DataFrame()
        
        use_consumption = (
        diff_day_session_consumption_df is not None
        and not diff_day_session_consumption_df.empty
        and 'timestamp' in diff_day_session_consumption_df.columns
    )
        
        ev_charger_session_df=session_df
        ev_charger_session_df['total_energy_kwh']=ev_charger_session_df['total_energy_kwh'].astype(float)/1000.0
        ev_charger_session_df['total_duration_min']=((pd.to_datetime(ev_charger_session_df['end_date']) - pd.to_datetime(ev_charger_session_df['start_date'])).dt.total_seconds())/60.0
        ev_charger_session_df['session_id']=ev_charger_session_df['source_id']

        ##Processing diff day session consumption to get supplied_energy_day per sub_session_id
        if use_consumption:
           
            diff_day_session_consumption_df = diff_day_session_consumption_df.copy()

            diff_day_session_consumption_df['timestamp'] = pd.to_datetime(
                diff_day_session_consumption_df['timestamp'], errors='coerce'
            )

            diff_day_session_consumption_df = (
                diff_day_session_consumption_df
                .dropna(subset=['timestamp'])
                .sort_values(['id', 'timestamp'])
            )

            
            last_record_per_day = (
                diff_day_session_consumption_df
                .groupby(['id', diff_day_session_consumption_df['timestamp'].dt.date])
                .tail(1)
                .copy()
            )

            # 3. daily energy
            last_record_per_day['prev_energy'] = (
                last_record_per_day.groupby('id')['energy'].shift(1)
            )

            last_record_per_day['supplied_energy_day'] = (
                last_record_per_day['energy']
                - last_record_per_day['prev_energy'].fillna(0)
            )

            # 4. sub_session_id
            last_record_per_day['counter'] = (
                last_record_per_day.groupby('id').cumcount() + 1
            )

            last_record_per_day['sub_session_id'] = (
                last_record_per_day['id'].astype(str)
                + "_"
                + last_record_per_day['counter'].map('{:02d}'.format)
            )

            last_record_per_day['timestamp'] = (
                last_record_per_day['timestamp'].dt.tz_localize(None)
            )

            energy_per_sub_session_df = last_record_per_day[
                ['sub_session_id', 'supplied_energy_day']
            ].copy()

        else:
            print("No diff-day session consumption — using fallback logic")

            energy_per_sub_session_df = pd.DataFrame(
                columns=['sub_session_id', 'supplied_energy_day']
            )

        ############### EV CHARGER SESSION DAILY ##################

        ev_charger_session_df['start_date'] = (
        pd.to_datetime(ev_charger_session_df['start_date']).dt.tz_localize(None)
        )
        ev_charger_session_df['end_date'] = (
            pd.to_datetime(ev_charger_session_df['end_date']).dt.tz_localize(None)
        )

        ev_charger_session_df = ev_charger_session_df.dropna(
            subset=['start_date', 'end_date']
        )

        rows = []

        for _, row in ev_charger_session_df.iterrows():
            s_start = row['start_date']
            s_end = row['end_date']

            date_range = pd.date_range(start=s_start.date(), end=s_end.date())

            for i, current_day in enumerate(date_range):
                day_start = max(pd.Timestamp(current_day), s_start)
                day_end = min(pd.Timestamp(current_day) + pd.Timedelta(days=1), s_end)

                day_duration = (day_end - day_start).total_seconds() / 60

                row_data = row.copy()
                row_data['id_day'] = current_day.strftime('%Y-%m-%d')
                row_data['sub_session_id'] = f"{row['session_id']}_{i+1:02}"
                row_data['total_duration_min'] = round(day_duration, 2)
                row_data['day_segment_start'] = day_start
                row_data['day_segment_end'] = day_end

                rows.append(row_data)

        ev_charger_session_daily_df = pd.DataFrame(rows)

        
        cols_to_drop = [
    'start_date', 'end_date',
    'socket_id', 'emsp', 'card_id'
]

        ev_charger_session_daily_df = ev_charger_session_daily_df.drop(
            columns=cols_to_drop, errors='ignore'
        )

        
        ev_charger_session_daily_df = ev_charger_session_daily_df.merge(
            energy_per_sub_session_df,
            on='sub_session_id',
            how='left'
        )

        ev_charger_session_daily_df['session_days'] = (
    ev_charger_session_daily_df
    .groupby('session_id')['session_id']
    .transform('count')
)
        # final energy
        def resolve_energy(row):
            # Caso 1 — veio da consumption (multi-day com dados)
            if pd.notnull(row.get('supplied_energy_day')):
                return row['supplied_energy_day']

            # Caso 2 — sessão single-day
            if row['session_days'] == 1:
                return row['total_energy_kwh']

            # Caso 3 — sessão multi-day sem consumption
            if row['sub_session_id'].endswith('_01'):
                return row['total_energy_kwh']

            # Caso 4 — dias intermédios
            return 0.0


        ev_charger_session_daily_df['total_energy_kwh'] = (
            ev_charger_session_daily_df.apply(resolve_energy, axis=1)
        )

        
        
        

        return ev_charger_session_df, ev_charger_session_daily_df
