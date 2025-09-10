import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime, date

# --- Load credentials ---
load_dotenv()
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL") or "postgresql://postgres.gyzsjokshqguccyfcbbi:avAsxDZpOMezTyo9@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"

# --- Page Configuration ---
st.set_page_config(
    page_title="Vessel EUAs & FuelEU Maritime Penalty Calculator",
    page_icon="🚢",
    layout="wide"
)

st.title("🚢 Vessel EUAs & FuelEU Maritime Penalty Calculator")
st.markdown("---")

# --- Initialize session state for caching ---
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

@st.cache_data
@st.cache_data
def load_data():
    """Load data from database with caching"""
    try:
        engine = create_engine(SUPABASE_DB_URL)
        
        # Fetch tables into DataFrames
        df_vessel = pd.read_sql("SELECT * FROM vessel_reports", con=engine)
        df_ports = pd.read_sql("SELECT * FROM \"Port_Name_List\"", con=engine)
        df_country = pd.read_sql("SELECT * FROM \"country_code_list\"", con=engine)
        
        # Data preprocessing
        df_vessel['phase_end_date'] = pd.to_datetime(df_vessel['phase_end_date'])
        df_vessel['date_str'] = df_vessel['phase_end_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Fill missing start_port with end_port where phase is ALL FAST
        df_vessel.loc[
            (df_vessel['phase'].str.upper() == 'ALL FAST') & (df_vessel['start_port'].isna()),
            'start_port'
        ] = df_vessel.loc[
            (df_vessel['phase'].str.upper() == 'ALL FAST') & (df_vessel['start_port'].isna()),
            'end_port'
        ]

        # Truncate ports
        df_vessel['start_port'] = df_vessel['start_port'].astype(str).str[:5]
        df_vessel['end_port'] = df_vessel['end_port'].astype(str).str[:5]

        # Ensure dataframe is sorted
        df_vessel.sort_values(['vessel_name', 'phase_end_date'], inplace=True)

        for idx in df_vessel[df_vessel['phase'] == 'EOSP'].index:
            vessel = df_vessel.at[idx, 'vessel_name']
            cargo_val = df_vessel.at[idx, 'cargo_mt']
            phase_end_date = df_vessel.at[idx, 'phase_end_date']
            
            # Find the LAST LINE just before this EOSP
            prev_rows = df_vessel[
                (df_vessel['vessel_name'] == vessel) &
                (df_vessel['phase'] == 'LAST LINE') &
                (df_vessel['phase_end_date'] < phase_end_date)
            ]
            
            if not prev_rows.empty:
                # Get the latest LAST LINE before EOSP
                last_idx = prev_rows.index[-1]
                if pd.isna(df_vessel.at[last_idx, 'cargo_mt']) or df_vessel.at[last_idx, 'cargo_mt'] == 0:
                    df_vessel.at[last_idx, 'cargo_mt'] = cargo_val

            # ----- Update ALL FAST after EOSP -----
            next_rows = df_vessel[
                (df_vessel['vessel_name'] == vessel) &
                (df_vessel['phase'] == 'ALL FAST') &
                (df_vessel['phase_end_date'] > phase_end_date)
            ]
            if not next_rows.empty:
                first_idx = next_rows.index[0]   # earliest ALL FAST after EOSP
                if pd.isna(df_vessel.at[first_idx, 'cargo_mt']) or df_vessel.at[first_idx, 'cargo_mt'] == 0:
                    df_vessel.at[first_idx, 'cargo_mt'] = cargo_val
        
        
        engine.dispose()
        return df_vessel, df_ports, df_country, df_vessel

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None, None, None, None


def get_database_engine():
    """Create database engine for operations"""
    return create_engine(SUPABASE_DB_URL)

# Load data
with st.spinner("Loading vessel data..."):
    df_vessel, df_ports, df_country, df_vessel = load_data()

if df_vessel is not None:
    st.session_state.data_loaded = True
    
    # --- Sidebar Controls ---
    st.sidebar.header("📊 Filter Options")
    
    # Vessel selection
    vessel_options = sorted(df_vessel['vessel_name'].unique().tolist())
    selected_vessel = st.sidebar.selectbox(
        "Select Vessel:",
        vessel_options,
        index=0 if vessel_options else None
    )
    
    # Date selection
    st.sidebar.subheader("📅 Date Range")
    
    # Get min and max dates from data
    min_date = df_vessel['phase_end_date'].min().date()
    max_date = df_vessel['phase_end_date'].max().date()
    
    from_date = st.sidebar.date_input(
        "From Date:",
        value=min_date,
        min_value=min_date,
        max_value=max_date
    )
    
    to_date = st.sidebar.date_input(
        "To Date:",
        value=max_date,
        min_value=from_date,  # Ensure to_date is not before from_date
        max_value=max_date
    )
    
    # --- Main Processing Function ---
    def process_vessel_data(vessel, from_date, to_date):
        """Process vessel data and calculate EUAs and penalties"""
        
        if not vessel or not from_date or not to_date:
            return None, None, None
            
        # Filter data
        mask = (
            (df_vessel['vessel_name'] == vessel) &
            (df_vessel['phase_end_date'].dt.date >= from_date) &
            (df_vessel['phase_end_date'].dt.date <= to_date) &
            (df_vessel['phase'].str.upper().isin(['ALL FAST', 'LAST LINE']))
        )
        
        final_df = df_vessel[mask].copy()
        
        if len(final_df) == 0:
            return None, None, None
            
        final_df['port'] = final_df['start_port']
        final_df = final_df.sort_values(by='phase_end_date').reset_index(drop=True)
        
        # Remove duplicates
        final_df = final_df.drop_duplicates(subset=[
            'phase_end_date', 'phase', 'start_port', 'end_port',
            'hfo_rob', 'lfo_rob', 'mgo_rob', 'lng_rob',
            'hfo_bdn', 'lfo_bdn', 'mgo_bdn', 'lng_bdn'
        ], keep='first').reset_index(drop=True)
        
        # Calculate fuel consumption
        for fuel in ['hfo', 'lfo', 'mgo', 'lng']:
            rob_col = f'{fuel}_rob'
            bdn_col = f'{fuel}_bdn'
            calc_col = f'cal_{fuel}_con'
            
            final_df[calc_col] = (
                final_df[rob_col].shift(1) + final_df[bdn_col] - final_df[rob_col]
            )
            final_df.loc[0, calc_col] = None
            
        for fuel in ['hfo', 'lfo', 'mgo', 'lng']:
                rob_col = f'{fuel}_rob'
                bdn_col = f'{fuel}_bdn'
                calc_col = f'cal_{fuel}_con'
                
                for i in range(1, len(final_df)):
                    current_consumption = final_df.at[i, calc_col]
                    current_bdn = final_df.at[i, bdn_col]
                    
                    # Check if consumption is negative and BDN is zero or missing
                    if (pd.notna(current_consumption) and current_consumption < 0 and 
                        (pd.isna(current_bdn) or current_bdn == 0)):
                        
                        current_date = final_df.at[i, 'phase_end_date']
                        
                        # Look for the most recent BDN before this date
                        prev_bdn_data = df_vessel[
                            (df_vessel['vessel_name'] == vessel) &
                            (df_vessel['phase_end_date'] < current_date) &
                            (df_vessel[bdn_col] > 0) &
                            (pd.notna(df_vessel[bdn_col]))
                        ].sort_values('phase_end_date')
                        
                        if not prev_bdn_data.empty:
                            # Get the most recent BDN value
                            latest_bdn = prev_bdn_data[bdn_col].iloc[-1]
                            final_df.at[i, bdn_col] = latest_bdn
                            
                            # Recalculate consumption with the new BDN value
                            if i > 0:  # Ensure we have a previous ROB value
                                final_df.at[i, calc_col] = (
                                    final_df.at[i - 1, rob_col] + 
                                    final_df.at[i, bdn_col] - 
                                    final_df.at[i, rob_col]
                                )
                            
                            print(f"Fixed {fuel.upper()} BDN for row {i}: Applied BDN value {latest_bdn}")
                        else:
                            print(f"Warning: No previous {fuel.upper()} BDN found for row {i} on {current_date}")
                    
                    elif (pd.notna(current_consumption) and current_consumption < -1000):
                        print(f"Warning: Extremely negative {fuel.upper()} consumption ({current_consumption}) at row {i}")
        
        # Remove cargo-matching AF-LL pairs
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
        
        if len(filtered_df_2) == 0:
            return None, None, None
        
        # Recalculate fuel consumption for filtered data
        for fuel in ['hfo', 'lfo', 'mgo', 'lng']:
            rob_col = f'{fuel}_rob'
            bdn_col = f'{fuel}_bdn'
            calc_col = f'cal_{fuel}_con'
            
            filtered_df_2[calc_col] = (
                filtered_df_2[rob_col].shift(1) + filtered_df_2[bdn_col] - filtered_df_2[rob_col]
            )
            filtered_df_2.loc[0, calc_col] = None

        for fuel in ['hfo', 'lfo', 'mgo', 'lng']:
            rob_col = f'{fuel}_rob'
            bdn_col = f'{fuel}_bdn'
            calc_col = f'cal_{fuel}_con'
                
            for i in range(1, len(filtered_df_2)):
                current_consumption = filtered_df_2.at[i, calc_col]
                current_bdn = filtered_df_2.at[i, bdn_col]
                    
                # Check if consumption is negative and BDN is zero or missing
                if (pd.notna(current_consumption) and current_consumption < 0 and 
                    (pd.isna(current_bdn) or current_bdn == 0)):
                        
                    current_date = filtered_df_2.at[i, 'phase_end_date']
                        
                    # Look for the most recent BDN before this date
                    prev_bdn_data = df_vessel[
                        (df_vessel['vessel_name'] == vessel) &
                        (df_vessel['phase_end_date'] < current_date) &
                        (df_vessel[bdn_col] > 0) &
                        (pd.notna(df_vessel[bdn_col]))
                    ].sort_values('phase_end_date')
                        
                    if not prev_bdn_data.empty:
                        # Get the most recent BDN value
                        latest_bdn = prev_bdn_data[bdn_col].iloc[-1]
                        filtered_df_2.at[i, bdn_col] = latest_bdn
                            
                        # Recalculate consumption with the new BDN value
                        if i > 0:  # Ensure we have a previous ROB value
                            filtered_df_2.at[i, calc_col] = (
                                filtered_df_2.at[i - 1, rob_col] + 
                                filtered_df_2.at[i, bdn_col] - 
                                filtered_df_2.at[i, rob_col]
                            )
                            
                        print(f"Fixed {fuel.upper()} BDN for row {i}: Applied BDN value {latest_bdn}")
                    else:
                        print(f"Warning: No previous {fuel.upper()} BDN found for row {i} on {current_date}")
                    
                elif (pd.notna(current_consumption) and current_consumption < -1000):
                    print(f"Warning: Extremely negative {fuel.upper()} consumption ({current_consumption}) at row {i}")
        
        # Calculate carbon emissions
        filtered_df_2['Carbon emitted'] = (
            filtered_df_2['cal_hfo_con'].fillna(0) * 3.114 +
            filtered_df_2['cal_lfo_con'].fillna(0) * 3.151 +
            filtered_df_2['cal_mgo_con'].fillna(0) * 3.206 +
            filtered_df_2['cal_lng_con'].fillna(0) * 2.75
        ).round(3)
        
        # Map EU membership
        df_country['port_code'] = df_country['country'].str[:2].str.upper()
        eu_mapping = dict(zip(df_country['country_code'].str.upper(), df_country['EU_membership']))
        
        filtered_df_2['Country Code'] = (
            filtered_df_2['port'].astype(str).str[:2].str.upper().map(eu_mapping).fillna('non-EU')
        )
        
        # Calculate EUAs
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


        cal_FuelEU_hfo_con = []
        for i in range(len(filtered_df_2)):
            if i == 0:
                cal_FuelEU_hfo_con.append(0.0)
            else:
                curr_country = filtered_df_2.loc[i, 'Country Code']
                prev_country = filtered_df_2.loc[i - 1, 'Country Code']
                curr_port = filtered_df_2.loc[i, 'port']
                prev_port = filtered_df_2.loc[i - 1, 'port']
                cal_hfo_con = filtered_df_2.loc[i, 'cal_hfo_con']

                if curr_port == prev_port:
                    cal_FuelEU_hfo_con.append(round(cal_hfo_con * 1, 3) if curr_country == 'EU' else 0.0)
                else:
                    if curr_country == 'EU' and prev_country == 'EU':
                        cal_FuelEU_hfo_con.append(round(cal_hfo_con * 1, 3))
                    elif curr_country == 'EU' or prev_country == 'EU':
                        cal_FuelEU_hfo_con.append(round(cal_hfo_con *  0.5, 3))
                    else:
                        cal_FuelEU_hfo_con.append(0.0)

        cal_FuelEU_lfo_con = []
        for i in range(len(filtered_df_2)):
            if i == 0:
                cal_FuelEU_lfo_con.append(0.0)
            else:
                curr_country = filtered_df_2.loc[i, 'Country Code']
                prev_country = filtered_df_2.loc[i - 1, 'Country Code']
                curr_port = filtered_df_2.loc[i, 'port']
                prev_port = filtered_df_2.loc[i - 1, 'port']
                cal_lfo_con = filtered_df_2.loc[i, 'cal_lfo_con']

                if curr_port == prev_port:
                    cal_FuelEU_lfo_con.append(round(cal_lfo_con * 1, 3) if curr_country == 'EU' else 0.0)
                else:
                    if curr_country == 'EU' and prev_country == 'EU':
                        cal_FuelEU_lfo_con.append(round(cal_lfo_con * 1, 3))
                    elif curr_country == 'EU' or prev_country == 'EU':
                        cal_FuelEU_lfo_con.append(round(cal_lfo_con *  0.5, 3))
                    else:
                        cal_FuelEU_lfo_con.append(0.0)

        cal_FuelEU_mgo_con = []
        for i in range(len(filtered_df_2)):
            if i == 0:
                cal_FuelEU_mgo_con.append(0.0)
            else:
                curr_country = filtered_df_2.loc[i, 'Country Code']
                prev_country = filtered_df_2.loc[i - 1, 'Country Code']
                curr_port = filtered_df_2.loc[i, 'port']
                prev_port = filtered_df_2.loc[i - 1, 'port']
                cal_mgo_con = filtered_df_2.loc[i, 'cal_mgo_con']

                if curr_port == prev_port:
                    cal_FuelEU_mgo_con.append(round(cal_mgo_con * 1, 3) if curr_country == 'EU' else 0.0)
                else:
                    if curr_country == 'EU' and prev_country == 'EU':
                        cal_FuelEU_mgo_con.append(round(cal_mgo_con * 1, 3))
                    elif curr_country == 'EU' or prev_country == 'EU':
                        cal_FuelEU_mgo_con.append(round(cal_mgo_con *  0.5, 3))
                    else:
                        cal_FuelEU_mgo_con.append(0.0)

        cal_FuelEU_lng_con = []
        for i in range(len(filtered_df_2)):
            if i == 0:
                cal_FuelEU_lng_con.append(0.0)
            else:
                curr_country = filtered_df_2.loc[i, 'Country Code']
                prev_country = filtered_df_2.loc[i - 1, 'Country Code']
                curr_port = filtered_df_2.loc[i, 'port']
                prev_port = filtered_df_2.loc[i - 1, 'port']
                cal_lng_con = filtered_df_2.loc[i, 'cal_lng_con']

                if curr_port == prev_port:
                    cal_FuelEU_lng_con.append(round(cal_lng_con * 1, 3) if curr_country == 'EU' else 0.0)
                else:
                    if curr_country == 'EU' and prev_country == 'EU':
                        cal_FuelEU_lng_con.append(round(cal_lng_con * 1, 3))
                    elif curr_country == 'EU' or prev_country == 'EU':
                        cal_FuelEU_lng_con.append(round(cal_lng_con *  0.5, 3))
                    else:
                        cal_FuelEU_lng_con.append(0.0)


        filtered_df_2['EUAs'] = EUAs
        filtered_df_2['cal_FuelEU_hfo_con'] = cal_FuelEU_hfo_con
        filtered_df_2['cal_FuelEU_lfo_con'] = cal_FuelEU_lfo_con
        filtered_df_2['cal_FuelEU_mgo_con'] = cal_FuelEU_mgo_con
        filtered_df_2['cal_FuelEU_lng_con'] = cal_FuelEU_lng_con
            
        
        # Calculate summary metrics
        start_date = filtered_df_2['phase_end_date'].min()
        end_date = filtered_df_2['phase_end_date'].max()
        total_co2 = round(filtered_df_2['Carbon emitted'].sum(), 3)
        total_eua = round(filtered_df_2['EUAs'].sum(), 3)
        
        # Now print total fuel consumption after the summary table
        total_hfo_cons = round(filtered_df_2['cal_FuelEU_hfo_con'].sum(skipna=True), 3)
        total_lfo_cons = round(filtered_df_2['cal_FuelEU_lfo_con'].sum(skipna=True), 3)
        total_mgo_cons = round(filtered_df_2['cal_FuelEU_mgo_con'].sum(skipna=True), 3)
        total_lng_cons = round(filtered_df_2['cal_FuelEU_lng_con'].sum(skipna=True), 3)
                
                
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
        if sum_2 == 0:
            WtT = 0
        else:
            WtT = sum_1/sum_2

        # Step 6: Calculating TtW
        sum_3 = a * HFO_TtW + b * LFO_TtW + c * MGO_TtW

        if sum_3 == 0 :
            TtW = 0
        else :
            TtW = sum_3/sum_2

        # Final Steps
        GHG_Ints_Act = WtT + TtW
        GHG_Ints_Tar = 89.3368
        value = abs(GHG_Ints_Act - GHG_Ints_Tar)

        if sum_2 == 0:
            CB_Def = 0
        else:
            CB_Def = (value * sum_2)

        if GHG_Ints_Act == 0:
            Penalty = 0
        else:
            Penalty = (CB_Def * 2400) / (GHG_Ints_Act * 41000)
        
        # Create summary tables
        eua_summary = pd.DataFrame({
            'From': [start_date.strftime('%Y-%m-%d %H:%M:%S')],
            'To': [end_date.strftime('%Y-%m-%d %H:%M:%S')],
            'CO₂ Emitted (mt)': [total_co2],
            'EUAs': [total_eua]
        })
        
        fueleu_summary = pd.DataFrame({
            'WtT': [round(WtT, 3)],
            'TtW': [round(TtW, 3)],
            'GHG_Ints_Act': [round(GHG_Ints_Act, 3)],
            'GHG_Ints_Tar': [round(GHG_Ints_Tar, 3)],
            'CB_Def': [-round(CB_Def, 3)],
            'Penalty (EUR)': [round(Penalty, 3)]
        })
        
        return filtered_df_2, eua_summary, fueleu_summary
    
    # --- Process Data and Display Results ---
    if from_date and to_date and selected_vessel:
        if from_date <= to_date:
            with st.spinner("Processing vessel data..."):
                result = process_vessel_data(selected_vessel, from_date, to_date)
                if result is not None:
                    filtered_df, eua_summary, fueleu_summary = result
                else:
                    filtered_df = eua_summary = fueleu_summary = None
            
            if filtered_df is not None and len(filtered_df) > 0:
                # Display key metrics at the top
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric(
                        "Total CO₂ Emitted", 
                        f"{eua_summary['CO₂ Emitted (mt)'].iloc[0]} mt",
                        help="Total carbon dioxide emissions in metric tonnes"
                    )
                
                with col2:
                    st.metric(
                        "EUAs Required", 
                        f"{eua_summary['EUAs'].iloc[0]}",
                        help="European Union Allowances required"
                    )
                
                with col3:
                    st.metric(
                        "GHG Intensity", 
                        f"{fueleu_summary['GHG_Ints_Act'].iloc[0]} g CO₂eq/MJ",
                        help="Actual greenhouse gas intensity"
                    )
                
                with col4:
                    penalty_value = fueleu_summary['Penalty (EUR)'].iloc[0]
                    st.metric(
                        "FuelEU Penalty", 
                        f"€{penalty_value:,.2f}",
                        help="FuelEU Maritime penalty in EUR"
                    )
                
                st.markdown("---")
                
                # Display detailed data
                st.subheader("📋 Detailed Voyage Data")
                
                display_columns = [
                    'phase_end_date', 'phase', 'Country Code', 'port', 'cargo_mt',
                    'hfo_rob', 'lfo_rob', 'mgo_rob', 'lng_rob',
                    'hfo_bdn', 'lfo_bdn', 'mgo_bdn', 'lng_bdn',
                    'cal_hfo_con', 'cal_lfo_con', 'cal_mgo_con', 'cal_lng_con',
                    'Carbon emitted', 'EUAs'
                ]
                
                st.dataframe(
                    filtered_df[display_columns],
                    use_container_width=True,
                    hide_index=True
                )
                
                # Display summary tables
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("🌍 EUA Calculation Summary")
                    st.dataframe(eua_summary, use_container_width=True, hide_index=True)
                
                with col2:
                    st.subheader("⛽ FuelEU Maritime Summary")
                    st.dataframe(fueleu_summary, use_container_width=True, hide_index=True)
                
                # Save to database button
                if st.button("💾 Save Results to Database", type="primary"):
                    try:
                        engine = get_database_engine()
                        
                        # Save filtered data
                        filtered_df.to_sql(
                            "Cape_Ferrol_EUA_Summary",
                            engine,
                            if_exists="replace",
                            index=False
                        )
                        
                        # Save FuelEU summary
                        fueleu_summary.to_sql(
                            "Cape_Ferrol_FuelEU_summary",
                            engine,
                            if_exists="replace",
                            index=False
                        )
                        
                        # Create merged dataset
                        row_repeated = pd.DataFrame([fueleu_summary.iloc[0]] * len(filtered_df)).reset_index(drop=True)
                        merged_df = pd.concat([filtered_df.reset_index(drop=True), row_repeated], axis=1)
                        
                        merged_df.to_sql(
                            "Cape_Ferrol_Merged",
                            engine,
                            if_exists="replace",
                            index=False
                        )
                        
                        engine.dispose()
                        
                        st.success("✅ Results saved to database successfully!")
                        
                    except Exception as e:
                        st.error(f"❌ Error saving to database: {str(e)}")
                
            else:
                st.warning("⚠️ No data found for the selected vessel and date range.")
                st.info("Please try selecting a different vessel or adjusting the date range.")
        
        else:
            st.error("❌ 'To Date' must be on or after 'From Date'.")
    
    else:
        st.info("👆 Please select a vessel and date range from the sidebar to view results.")

else:
    st.error("❌ Failed to load data from database. Please check your connection settings.")
    st.info("Make sure your SUPABASE_DB_URL environment variable is correctly set.")

# --- Footer ---
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; font-size: 0.8em;'>
    🚢 Vessel EUAs & FuelEU Maritime Penalty Calculator | 
    Built with Streamlit
    </div>
    """, 
    unsafe_allow_html=True
)


