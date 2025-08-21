load_dotenv()
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL") or "postgresql://postgres.gyzsjokshqguccyfcbbi:avAsxDZpOMezTyo9@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
 
# --- SQLAlchemy engine (for pandas) ---
engine = create_engine(SUPABASE_DB_URL)
 
# --- Fetch tables into DataFrames ---
df_vessel = pd.read_sql("SELECT * FROM vessel_reports", con=engine)
df_ports = pd.read_sql("SELECT * FROM \"Port_Name_List\"", con=engine)  # Quotes if camel case or uppercase
df_country = pd.read_sql("SELECT * FROM \"country_code_list\"", con=engine)
df_CapeFerrol = pd.read_sql("SELECT * FROM \"cape_ferrol\"", con =engine)

# Ensure phase_end_date is datetime
df_CapeFerrol['phase_end_date'] = pd.to_datetime(df_CapeFerrol['phase_end_date'])
df_CapeFerrol['date_str'] = df_CapeFerrol['phase_end_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Fill missing start_port with end_port where phase is ALL FAST
df_CapeFerrol.loc[
    (df_CapeFerrol['phase'].str.upper() == 'ALL FAST') & (df_CapeFerrol['start_port'].isna()),
    'start_port'
] = df_CapeFerrol.loc[
    (df_CapeFerrol['phase'].str.upper() == 'ALL FAST') & (df_CapeFerrol['start_port'].isna()),
    'end_port'
]

# Truncate ports
df_CapeFerrol['start_port'] = df_CapeFerrol['start_port'].astype(str).str[:5]
df_CapeFerrol['end_port'] = df_CapeFerrol['end_port'].astype(str).str[:5]

# Widget Setup
vessel_dropdown = widgets.Dropdown(
    options=sorted(df_CapeFerrol['vessel_name'].unique().tolist()),
    description='Select Vessel:',
    style={'description_width': 'initial'},
    layout=widgets.Layout(width='50%')
)

from_date_picker = widgets.DatePicker(
    description='From Date:',
    disabled=False,
    style={'description_width': 'initial'}
)

to_date_picker = widgets.DatePicker(
    description='To Date:',
    disabled=False,
    style={'description_width': 'initial'}
)

output_box = widgets.Output()

# Function to enforce from-to date rule
def enforce_to_date_limit(*args):
    from_date = from_date_picker.value
    to_date = to_date_picker.value

    if from_date:
        if to_date and to_date < from_date:
            to_date_picker.value = None
        to_date_picker.description = f"To Date (> {from_date}):"
    else:
        to_date_picker.description = "To Date:"

    display_filtered_data()

# Function to display filtered data
def display_filtered_data(*args):
    output_box.clear_output()
    with output_box:
        vessel = vessel_dropdown.value
        from_date = from_date_picker.value
        to_date = to_date_picker.value

        if from_date and to_date:
            from_date_str = from_date.strftime('%Y-%m-%d')
            to_date_str = to_date.strftime('%Y-%m-%d')

            mask = (
                (df_CapeFerrol['vessel_name'] == vessel) &
                (df_CapeFerrol['phase_end_date'].dt.date >= from_date) &
                (df_CapeFerrol['phase_end_date'].dt.date <= to_date) &
                (df_CapeFerrol['phase'].str.upper().isin(['ALL FAST', 'LAST LINE']))
            )

            final_df = df_CapeFerrol[mask].copy()
            final_df['port'] = final_df['start_port']
            final_df = final_df.sort_values(by='phase_end_date').reset_index(drop=True)

            final_df = final_df.drop_duplicates(subset=[
                'phase_end_date', 'phase', 'start_port', 'end_port',
                'hfo_rob', 'lfo_rob', 'mgo_rob', 'lng_rob',
                'hfo_bdn', 'lfo_bdn', 'mgo_bdn', 'lng_bdn'
            ], keep='first').reset_index(drop=True)

            for fuel in ['hfo', 'lfo', 'mgo', 'lng']:
                rob_col = f'{fuel}_rob'
                bdn_col = f'{fuel}_bdn'
                calc_col = f'cal_{fuel}_con'

                final_df[calc_col] = (
                    final_df[rob_col].shift(1) + final_df[bdn_col] - final_df[rob_col]
                )
                final_df.loc[0, calc_col] = None

                for i in range(1, len(final_df)):
                    if final_df.at[i, calc_col] < 0 and final_df.at[i, bdn_col] == 0:
                        current_date = final_df.at[i, 'phase_end_date']
                        next_bdn = df_CapeFerrol[
                            (df_CapeFerrol['vessel_name'] == vessel) &
                            (df_CapeFerrol['phase_end_date'] > current_date) &
                            (df_CapeFerrol[bdn_col] > 0)
                        ].sort_values('phase_end_date')[bdn_col].head(1)

                        if not next_bdn.empty:
                            final_df.at[i, bdn_col] = next_bdn.values[0]
                            final_df.at[i, calc_col] = (
                                final_df.at[i - 1, rob_col] + final_df.at[i, bdn_col] - final_df.at[i, rob_col]
                            )

            print(f"\n Display 1: All 'ALL FAST' & 'LAST LINE' Data from {from_date_str} to {to_date_str} for '{vessel}':")
            display(final_df[[
                'phase_end_date', 'phase', 'port', 'cargo_mt',
                'hfo_rob', 'lfo_rob', 'mgo_rob', 'lng_rob',
                'hfo_bdn', 'lfo_bdn', 'mgo_bdn', 'lng_bdn',
                'cal_hfo_con', 'cal_lfo_con', 'cal_mgo_con', 'cal_lng_con'
            ]])

            rows_to_remove = []
            i = 0
            while i < len(final_df) - 1:
                row_af = final_df.iloc[i]
                row_ll = final_df.iloc[i + 1]
                if row_af['phase'].upper() == 'ALL FAST' and row_ll['phase'].upper() == 'LAST LINE':
                    if row_af['cargo_mt'] == row_ll['cargo_mt']:
                        rows_to_remove.extend([i, i + 1])
                    i += 2
                else:
                    i += 1

            filtered_df_2 = final_df.drop(index=rows_to_remove).reset_index(drop=True)

            filtered_df_2.to_sql(
                "Cape_Ferrol_EUA_Summary",  # Table name in MySQL
                engine,
                if_exists="replace",  # Change to 'replace' if you want to overwrite instead of append
                index=False
            )

            for fuel in ['hfo', 'lfo', 'mgo', 'lng']:
                rob_col = f'{fuel}_rob'
                bdn_col = f'{fuel}_bdn'
                calc_col = f'cal_{fuel}_con'

                filtered_df_2[calc_col] = (
                    filtered_df_2[rob_col].shift(1) + filtered_df_2[bdn_col] - filtered_df_2[rob_col]
                )
                filtered_df_2.loc[0, calc_col] = None

                for i in range(1, len(filtered_df_2)):
                    if filtered_df_2.at[i, calc_col] < 0 and filtered_df_2.at[i, bdn_col] == 0:
                        current_date = filtered_df_2.at[i, 'phase_end_date']
                        next_bdn = df_CapeFerrol[
                            (df_CapeFerrol['vessel_name'] == vessel) &
                            (df_CapeFerrol['phase_end_date'] > current_date) &
                            (df_CapeFerrol[bdn_col] > 0)
                        ].sort_values('phase_end_date')[bdn_col].head(1)

                        if not next_bdn.empty:
                            filtered_df_2.at[i, bdn_col] = next_bdn.values[0]
                            filtered_df_2.at[i, calc_col] = (
                                filtered_df_2.at[i - 1, rob_col] + filtered_df_2.at[i, bdn_col] - filtered_df_2.at[i, rob_col]
                            )

            filtered_df_2['Carbon emitted'] = (
                filtered_df_2['cal_hfo_con'].fillna(0) * 3.114 +
                filtered_df_2['cal_lfo_con'].fillna(0) * 3.151 +
                filtered_df_2['cal_mgo_con'].fillna(0) * 3.206 +
                filtered_df_2['cal_lng_con'].fillna(0) * 2.75
            ).round(3)

            df_country['port_code'] = df_country['country'].str[:2].str.upper()
            eu_mapping = dict(zip(df_country['country_code'].str.upper(), df_country['EU_membership']))

            filtered_df_2['Country Code'] = (
                filtered_df_2['port'].astype(str).str[:2].str.upper().map(eu_mapping).fillna('non-EU')
            )

            EUAs = []
            for i in range(len(filtered_df_2)):
                if i == 0:
                    EUAs.append(0.0)
                else:
                    curr_country = filtered_df_2.loc[i, 'Country Code']
                    prev_country = filtered_df_2.loc[i - 1, 'Country Code']
                    curr_port = filtered_df_2.loc[i, 'port']
                    prev_port = filtered_df_2.loc[i - 1, 'port']
                    carbon_emitted = filtered_df_2.loc[i, 'Carbon emitted']

                    if curr_port == prev_port:
                        EUAs.append(round(carbon_emitted * 0.7, 3) if curr_country == 'EU' else 0.0)
                    else:
                        if curr_country == 'EU' and prev_country == 'EU':
                            EUAs.append(round(carbon_emitted * 0.7, 3))
                        elif curr_country == 'EU' or prev_country == 'EU':
                            EUAs.append(round(carbon_emitted * 0.7 * 0.5, 3))
                        else:
                            EUAs.append(0.0)

            filtered_df_2['EUAs'] = EUAs

            print(f"\n Display 2: Cargo-Changing Legs Only (Excludes same cargo_mt AF–LL pairs):")
            display(filtered_df_2[[
                'phase_end_date', 'phase', 'Country Code', 'port', 'cargo_mt',
                'hfo_rob', 'lfo_rob', 'mgo_rob', 'lng_rob',
                'hfo_bdn', 'lfo_bdn', 'mgo_bdn', 'lng_bdn',
                'cal_hfo_con', 'cal_lfo_con', 'cal_mgo_con', 'cal_lng_con',
                'Carbon emitted', 'EUAs'
            ]])            

            
            if len(filtered_df_2) > 0:
                start_date = filtered_df_2['phase_end_date'].min()
                end_date = filtered_df_2['phase_end_date'].max()
                total_co2 = round(filtered_df_2['Carbon emitted'].sum(), 3)
                total_eua = round(filtered_df_2['EUAs'].sum(), 3)

                table_3 = pd.DataFrame({
                    'From': [start_date.strftime('%Y-%m-%d %H:%M:%S')],
                    'To': [end_date.strftime('%Y-%m-%d %H:%M:%S')],
                    'CO₂ Emitted (mt)': [total_co2],
                    'EUAs': [total_eua]
                })

                styled_html = table_3.to_html(index=False, border=1, justify='center', classes='styled-table')

                css = """
                <style>
                .styled-table {
                    border-collapse: collapse;
                    margin: 10px 0;
                    font-size: 15px;
                    font-family: sans-serif;
                    min-width: 400px;
                    border: 1px solid #dddddd;
                }
                .styled-table th, .styled-table td {
                    padding: 12px 15px;
                    border: 1px solid #dddddd;
                    text-align: center;
                }
                .styled-table thead tr {
                    background-color: #009879;
                    color: #ffffff;
                    text-align: center;
                    font-weight: bold;
                }
                </style>
                """
                print("\nSummary Table: Total CO₂ and EUA between date range\n")
                display(HTML(f"""
                <div style="background-color:#cce5ff; padding:10px; border-radius:5px; border:1px solid #99ccff;">
                <b>EUA Calculation
                """))
                display(HTML(css + styled_html))

                
                # Now print total fuel consumption after the summary table
                total_hfo_cons = round(filtered_df_2['cal_hfo_con'].sum(skipna=True), 3)
                total_lfo_cons = round(filtered_df_2['cal_lfo_con'].sum(skipna=True), 3)
                total_mgo_cons = round(filtered_df_2['cal_mgo_con'].sum(skipna=True), 3)
                total_lng_cons = round(filtered_df_2['cal_lng_con'].sum(skipna=True), 3)
                
                
                # Step 1: Convert to micro-units (tonnes * 10^6)
                a = total_hfo_cons * 10**6
                b = total_lfo_cons * 10**6
                c = total_mgo_cons * 10**6
                
                # Step 2: Reference table values
                HFO_CO2_eqv = 13.5
                LFO_CO2_eqv = 13.2
                MGO_CO2_eqv = 14.4
                
                HFO_LCV = 0.0405
                LFO_LCV = 0.041
                MGO_LCV = 0.0427

                HFO_TtW = 3.16889
                LFO_TtW = 3.20589
                MGO_TtW = 3.26089
                
                
                # Step 3: Calculate sum 1 (CO₂ equivalent WtT)
                sum_1 = (a * HFO_CO2_eqv * HFO_LCV) + (b * LFO_CO2_eqv * LFO_LCV) + (c * MGO_CO2_eqv * MGO_LCV)
                
                # Step 4: Calculate sum 2 (LCV)
                sum_2 = (a * HFO_LCV) + (b * LFO_LCV) + (c * MGO_LCV)

                # Step 5: Calculating WtT
                WtT = sum_1/sum_2

                # Step 6: Calculating TtW
                sum_3 = a * HFO_TtW + b * LFO_TtW + c * MGO_TtW
                TtW = sum_3/sum_2

                # Final Steps
                GHG_Ints_Act = WtT + TtW
                GHG_Ints_Tar = 89.3368
                value = abs(GHG_Ints_Act - GHG_Ints_Tar)
                CB_Def = (value * sum_2)
                Penalty = (CB_Def * 2400) / (GHG_Ints_Act * 41000)



                # Create a DataFrame for the second summary
                table_4 = pd.DataFrame({
                    'WtT': [round(WtT, 3)],
                    'TtW': [round(TtW, 3)],
                    'GHG_Ints_Act': [round(GHG_Ints_Act, 3)],
                    'GHG_Ints_Tar': [round(GHG_Ints_Tar, 3)],
                    'CB_Def': [-round(CB_Def, 3)],
                    'Penalty (EUR)': [round(Penalty, 3)]
                })

                display(HTML(f"""
                <div style="background-color:#cce5ff; padding:10px; border-radius:5px; border:1px solid #99ccff;">
                <b>FuelEU Calculation
                """))
               
                # Use same CSS styling
                styled_html_2 = table_4.to_html(index=False, border=1, justify='center', classes='styled-table')
                display(HTML(css + styled_html_2))
                table_4.to_sql("Cape_Ferrol_FuelEU_summary",  # Table name in MySQL
                engine,
                if_exists="replace",  # Change to 'replace' if you want to overwrite instead of append
                index=False
                )

                # Take the first row from table_4 and repeat to match filtered_df_2 length
                row_2nd_repeated = pd.DataFrame([table_4.iloc[0]] * len(filtered_df_2)).reset_index(drop=True)
                
                # Merge both DataFrames side-by-side
                merged_df = pd.concat([filtered_df_2.reset_index(drop=True), row_2nd_repeated], axis=1)
                
                # Save merged DataFrame to MySQL (or SQLite)
                merged_df.to_sql(
                    "Cape_Ferrol_Merged",  # New table name
                    engine,                # Your DB engine
                    if_exists="replace",   # Overwrite if table already exists
                    index=False
                )

            else:
                print("\nNo data available for summary table.")

        else:
            print("Please select both From Date and To Date (To Date must be on/after From Date).")



vessel_dropdown.observe(display_filtered_data, names='value')
from_date_picker.observe(enforce_to_date_limit, names='value')
to_date_picker.observe(display_filtered_data, names='value')

display(widgets.VBox([
    vessel_dropdown,
    widgets.HBox([from_date_picker, to_date_picker]),
    output_box
]))