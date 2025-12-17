import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime, date
import numpy as np

# --- Load credentials ---
load_dotenv()
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL") or "postgresql://postgres.gyzsjokshqguccyfcbbi:avAsxDZpOMezTyo9@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"

# --- Page Configuration ---
st.set_page_config(
    page_title="Vessel EUAs & FuelEU Maritime Penalty Calculator",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="expanded" 
)

st.markdown("""
<style>
    /* ===== COHESIVE COLOR PALETTE ===== */
    :root {
        --primary-color: #0052cc;
        --primary-light: #0066ff;
        --primary-dark: #003d99;
        --secondary-color: #00d4ff;
        --secondary-dark: #00a8cc;
        --accent-color: #ff6b35;
        --accent-light: #ff8c5a;
        --success-color: #00d084;
        --success-dark: #00a366;
        --dark-bg: #0a0e27;
        --card-bg: #1a1f3a;
        --card-bg-light: #252d4a;
        --card-bg-hover: #2d3555;
        --text-primary: #e8eef7;
        --text-secondary: #a0aac0;
        --border-color: #2a3050;
        --border-light: #3a4560;
    }
    
    /* ===== GLOBAL STYLES ===== */
    * {
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    
    html, body {
        background-color: var(--dark-bg) !important;
    }
    
    [data-testid="stAppViewContainer"] {
        background-color: var(--dark-bg) !important;
    }
    
    [data-testid="stSidebar"] {
        background-color: var(--card-bg) !important;
    }
    
    [data-testid="stContainer"],
    [data-testid="stDataFrame"],
    .stDataFrame,
    [data-testid="column"],
    .element-container,
    .stMarkdown {
        background-color: transparent !important;
        border: none !important;
    }
    
    /* ===== MAIN HEADER ===== */
    .main-header {
        background: linear-gradient(135deg, #0052cc 0%, #0066ff 50%, #00d4ff 100%);
        padding: 2.5rem 2rem;
        border-radius: 16px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(0, 82, 204, 0.25);
        position: relative;
        overflow: hidden;
    }
    
    .main-header::before {
        content: '';
        position: absolute;
        top: -50%;
        right: -10%;
        width: 400px;
        height: 400px;
        background: rgba(255, 255, 255, 0.1);
        border-radius: 50%;
        filter: blur(40px);
    }
    
    .main-header h1 {
        margin: 0;
        font-size: 2.4rem;
        font-weight: 800;
        text-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
        position: relative;
    }
    
    .main-header p {
        margin: 0.5rem 0 0 0;
        font-size: 1.1rem;
        opacity: 0.95;
        position: relative;
    }
    
    /* ===== CARDS & CONTAINERS ===== */
    .info-card {
        background: var(--card-bg);
        border-radius: 12px;
        padding: 1.5rem;
        border: 1px solid var(--border-color);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
        transition: all 0.3s ease;
    }
    
    .info-card:hover {
        background: var(--card-bg-hover);
        border-color: var(--primary-color);
        box-shadow: 0 6px 20px rgba(0, 82, 204, 0.3);
        transform: translateY(-2px);
    }
    
    .info-card h3 {
        color: var(--secondary-color);
        font-size: 1.1rem;
        font-weight: 700;
        margin: 0 0 0.5rem 0;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .info-card p {
        color: var(--text-primary);
        font-size: 2rem;
        font-weight: 800;
        margin: 0;
    }
    
    /* ===== SIDEBAR FILTERS ===== */
    .sidebar-section {
        background: var(--card-bg-light);
        padding: 1.25rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        border: 1px solid var(--border-color);
    }
    
    .sidebar-section h3 {
        color: var(--primary-light);
        font-size: 1rem;
        font-weight: 700;
        margin: 0 0 1rem 0;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    /* ===== BUTTONS ===== */
    .stButton > button {
        background: linear-gradient(135deg, var(--primary-color) 0%, var(--primary-dark) 100%);
        color: white;
        border: none;
        padding: 0.75rem 2rem;
        font-weight: 600;
        border-radius: 8px;
        transition: all 0.3s ease;
        box-shadow: 0 4px 12px rgba(0, 82, 204, 0.3);
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, var(--primary-light) 0%, var(--primary-color) 100%);
        box-shadow: 0 6px 20px rgba(0, 102, 255, 0.4);
        transform: translateY(-2px);
    }
    
    /* ===== DATAFRAME STYLING ===== */
    .dataframe {
        background-color: var(--card-bg) !important;
        border-radius: 8px;
        border: 1px solid var(--border-color) !important;
        color: var(--text-primary) !important;
    }
    
    .dataframe thead th {
        background-color: var(--primary-dark) !important;
        color: white !important;
        font-weight: 700;
        text-transform: uppercase;
        font-size: 0.85rem;
        padding: 0.75rem !important;
        border: none !important;
    }
    
    .dataframe tbody tr {
        background-color: var(--card-bg) !important;
        border-bottom: 1px solid var(--border-color) !important;
        transition: all 0.2s ease;
    }
    
    .dataframe tbody tr:hover {
        background-color: var(--card-bg-hover) !important;
    }
    
    .dataframe tbody td {
        color: var(--text-primary) !important;
        padding: 0.75rem !important;
        border: none !important;
    }
    
    /* ===== TABS ===== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: var(--card-bg);
        padding: 0.5rem;
        border-radius: 10px;
        border: 1px solid var(--border-color);
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        color: var(--text-secondary);
        border-radius: 6px;
        padding: 0.75rem 1.5rem;
        font-weight: 600;
        border: none;
        transition: all 0.3s ease;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background-color: var(--card-bg-light);
        color: var(--primary-light);
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, var(--primary-color) 0%, var(--primary-dark) 100%);
        color: white !important;
        box-shadow: 0 4px 12px rgba(0, 82, 204, 0.3);
    }
    
    /* ===== INPUT FIELDS ===== */
    .stSelectbox label, .stDateInput label, .stTextInput label {
        color: var(--text-secondary) !important;
        font-weight: 600;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .stSelectbox > div > div, .stDateInput > div > div, .stTextInput > div > div {
        background-color: var(--card-bg-light);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        color: var(--text-primary);
    }
    
    .stSelectbox > div > div:hover, .stDateInput > div > div:hover, .stTextInput > div > div:hover {
        border-color: var(--primary-color);
        box-shadow: 0 0 0 1px var(--primary-color);
    }
    
    /* ===== ALERTS ===== */
    .stAlert {
        background-color: var(--card-bg);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        color: var(--text-primary);
    }
    
    /* ===== METRICS ===== */
    [data-testid="stMetricValue"] {
        color: var(--text-primary);
        font-size: 2rem;
        font-weight: 800;
    }
    
    [data-testid="stMetricLabel"] {
        color: var(--text-secondary);
        font-weight: 600;
        text-transform: uppercase;
        font-size: 0.85rem;
        letter-spacing: 0.5px;
    }
</style>
""", unsafe_allow_html=True)

# --- Header Section ---
st.markdown("""
<div class="main-header">
    <h1>🚢 Vessel EUAs & FuelEU Maritime Penalty Calculator</h1>
    <p>Advanced emissions tracking and compliance analysis for maritime operations</p>
</div>
""", unsafe_allow_html=True)

# --- Initialize session state for caching ---
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

@st.cache_data
def load_data():
    """Load data from database with caching and proper type conversions"""
    try:
        engine = create_engine(SUPABASE_DB_URL)
        
        # Fetch tables into DataFrames
        df_vessel = pd.read_sql("SELECT * FROM vessel_reports", con=engine)
        df_ports = pd.read_sql("SELECT * FROM \"Port_Name_List\"", con=engine)
        df_country = pd.read_sql("SELECT * FROM \"country_code_list\"", con=engine)
        df_vessel_type = pd.read_sql("SELECT vessel_name, vessel_type FROM vessels_type_list", con=engine)

        numeric_columns = [
            'hfo_rob', 'lfo_rob', 'mgo_rob', 'lng_rob',
            'hfo_bdn', 'lfo_bdn', 'mgo_bdn', 'lng_bdn',
            'cargo_mt'
        ]
        
        for col in numeric_columns:
            if col in df_vessel.columns:
                df_vessel[col] = pd.to_numeric(df_vessel[col], errors='coerce').fillna(0)
        
        # Normalize
        df_vessel_type['vessel_name'] = df_vessel_type['vessel_name'].str.strip()
        df_vessel_type['vessel_type'] = df_vessel_type['vessel_type'].str.strip().str.upper()
        
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
        return df_vessel, df_ports, df_country, df_vessel_type
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.error("Please check your database connection and ensure SUPABASE_DB_URL is set correctly.")
        return None, None, None, None

def get_database_engine():
    """Create database engine for operations"""
    return create_engine(SUPABASE_DB_URL)

# Load data
try:
    df_vessel, df_ports, df_country, df_vessel_type = load_data()
    
    if df_vessel is None:
        st.stop()
        
    st.session_state.data_loaded = True
except Exception as e:
    st.error(f"Failed to load data: {str(e)}")
    st.stop()

# Get unique vessel names
vessel_names = sorted(df_vessel['vessel_name'].unique().tolist())

# --- Sidebar Filters ---
with st.sidebar:
    st.markdown('<div class="sidebar-section"><h3>📊 Filter Options</h3>', unsafe_allow_html=True)
    
    st.markdown("### Select Vessel:")
    selected_vessel = st.selectbox(
        "Choose a vessel",
        options=vessel_names,
        index=0 if vessel_names else None,
        label_visibility="collapsed"
    )
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('<div class="sidebar-section"><h3>📅 Date Range</h3>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**From Date:**")
        from_date = st.date_input(
            "From",
            value=date(2024, 12, 30),
            label_visibility="collapsed"
        )
    
    with col2:
        st.markdown("**To Date:**")
        to_date = st.date_input(
            "To",
            value=date(2025, 12, 17),
            label_visibility="collapsed"
        )
    
    st.markdown('</div>', unsafe_allow_html=True)

# --- Main Processing Function ---
@st.cache_data
def process_vessel_data(vessel, from_date, to_date):
    """Process vessel data and calculate EUAs and penalties with proper type handling"""
    
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
    
    numeric_cols = ['hfo_rob', 'lfo_rob', 'mgo_rob', 'lng_rob', 
                   'hfo_bdn', 'lfo_bdn', 'mgo_bdn', 'lng_bdn', 'cargo_mt']
    for col in numeric_cols:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0).astype('float64')
    
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
        
        # initialize consumption with float64
        final_df[calc_col] = np.nan
        final_df[calc_col] = final_df[calc_col].astype('float64')
        
        for i in range(1, len(final_df)):
            prev_date = final_df.at[i-1, 'phase_end_date']
            curr_date = final_df.at[i, 'phase_end_date']
        
            # Sum BDN values between (prev_date, curr_date]
            bdn_sum = df_vessel[
                (df_vessel['vessel_name'] == vessel) &
                (df_vessel['phase_end_date'] > prev_date) &
                (df_vessel['phase_end_date'] <= curr_date)
            ][bdn_col].sum(min_count=1)
        
            if pd.isna(bdn_sum):
                bdn_sum = 0.0
            else:
                bdn_sum = float(bdn_sum)
        
            # Update BDN for current row
            final_df.at[i, bdn_col] = bdn_sum
        
            # Calculate consumption with explicit float conversion
            prev_rob = float(final_df.at[i-1, rob_col])
            curr_rob = float(final_df.at[i, rob_col])
            final_df.at[i, calc_col] = prev_rob + bdn_sum - curr_rob
    
    # Get vessel type
    vessel_type_match = df_vessel_type[df_vessel_type['vessel_name'] == vessel]
    vessel_type = vessel_type_match['vessel_type'].iloc[0] if not vessel_type_match.empty else None
    
    # Remove cargo-matching AF-LL pairs
    rows_to_remove = []

    if vessel_type and 'container' in vessel_type.lower():
        # mark MAPTM and EGPSE rows for removal
        for i in range(len(final_df)):
            if final_df.at[i, 'port'] in ['MAPTM', 'EGPSE']:
                rows_to_remove.append(i)
        
    i = 0
    while i < len(final_df) - 1:
        row_af = final_df.iloc[i]
        row_ll = final_df.iloc[i + 1]
        if row_af['phase'] == 'ALL FAST' and row_ll['phase'] == 'LAST LINE':
            if row_af['cargo_mt'] == row_ll['cargo_mt']:
                rows_to_remove.extend([i, i + 1])
            i += 2
        else:
            i += 1

    filtered_df_2 = final_df.drop(index=rows_to_remove).reset_index(drop=True)

    for col in numeric_cols:
        if col in filtered_df_2.columns:
            filtered_df_2[col] = pd.to_numeric(filtered_df_2[col], errors='coerce').fillna(0).astype('float64')

    # Recalculate bunker values for Display 2
    for fuel in ['hfo', 'lfo', 'mgo', 'lng']:
        rob_col = f'{fuel}_rob'
        bdn_col = f'{fuel}_bdn'
        calc_col = f'cal_{fuel}_con'

        # initialize consumption with float64
        filtered_df_2[calc_col] = np.nan
        filtered_df_2[calc_col] = filtered_df_2[calc_col].astype('float64')
    
        for i in range(1, len(filtered_df_2)):
            prev_date = filtered_df_2.at[i-1, 'phase_end_date']
            curr_date = filtered_df_2.at[i, 'phase_end_date']
    
            # Sum BDN values between (prev_date, curr_date]
            bdn_sum = df_vessel[
                (df_vessel['vessel_name'] == vessel) &
                (df_vessel['phase_end_date'] > prev_date) &
                (df_vessel['phase_end_date'] <= curr_date)
            ][bdn_col].sum(min_count=1)
    
            if pd.isna(bdn_sum):
                bdn_sum = 0.0
            else:
                bdn_sum = float(bdn_sum)
    
            # Update BDN for current row
            filtered_df_2.at[i, bdn_col] = bdn_sum
    
            # Calculate consumption with explicit float conversion
            prev_rob = float(filtered_df_2.at[i-1, rob_col])
            curr_rob = float(filtered_df_2.at[i, rob_col])
            filtered_df_2.at[i, calc_col] = prev_rob + bdn_sum - curr_rob

    port_to_country = df_ports.set_index('Port Code')['EU Ports'].to_dict()
    port_to_omr = df_ports.set_index('Port Code')['OMR'].to_dict()

    filtered_df_2['Country Code'] = filtered_df_2['port'].map(port_to_country)
    filtered_df_2['OMR'] = filtered_df_2['port'].map(port_to_omr)
    
    filtered_df_2['Carbon emitted'] = (
        filtered_df_2['cal_hfo_con'].fillna(0.0).astype('float64') * 3.114 +
        filtered_df_2['cal_lfo_con'].fillna(0.0).astype('float64') * 3.151 +
        filtered_df_2['cal_mgo_con'].fillna(0.0).astype('float64') * 3.206 +
        filtered_df_2['cal_lng_con'].fillna(0.0).astype('float64') * 2.75
    ).round(3)

    
    EUAs = []

    for i in range(len(filtered_df_2)):
        if i == 0:
            EUAs.append(0.0)
            continue
    
        curr_country = filtered_df_2.loc[i, 'Country Code']
        prev_country = filtered_df_2.loc[i - 1, 'Country Code']
        curr_port = filtered_df_2.loc[i, 'port']
        prev_port = filtered_df_2.loc[i - 1, 'port']
        curr_OMR = filtered_df_2.loc[i, 'OMR']
        prev_OMR = filtered_df_2.loc[i - 1, 'OMR']
        carbon_emitted = float(filtered_df_2.loc[i, 'Carbon emitted'])
    
        # ---- Safe forward look ----
        next_country = filtered_df_2.loc[i + 1, 'Country Code'] if i < len(filtered_df_2) - 1 else None
        next_OMR = filtered_df_2.loc[i + 1, 'OMR'] if i < len(filtered_df_2) - 1 else None
    
        # ---- Safe backward look ----
        two_back_prev_country = filtered_df_2.loc[i - 2, 'Country Code'] if i >= 2 else None
        two_back_prev_OMR = filtered_df_2.loc[i - 2, 'OMR'] if i >= 2 else None
    
        # ---- Safe forward 2 look ----
        two_fwd_next_country = filtered_df_2.loc[i + 2, 'Country Code'] if i < len(filtered_df_2) - 2 else None
        two_fwd_next_OMR = filtered_df_2.loc[i + 2, 'OMR'] if i < len(filtered_df_2) - 2 else None
    
        # ===================================================================
        #                         MAIN EUA LOGIC 
        # ===================================================================
    
        # ---------- Case 1: Port Consumption ----------
        if curr_port == prev_port:
            if curr_country == 'EU' and two_back_prev_country == 'EU' and curr_OMR == 'No' and two_back_prev_OMR == 'Yes' :
                if curr_country[:2] == two_back_prev_country[:2] :
                    EUAs.append(0.0)
                else:
                    EUAs.append(round(carbon_emitted * 0.7, 3))
    
            elif curr_country == 'EU' and next_country == 'EU' and curr_OMR == 'No' and next_OMR == 'Yes' :
                if curr_country[:2] == next_country[:2]:
                    EUAs.append(0.0)
                else:
                    EUAs.append(round(carbon_emitted * 0.7, 3))

            elif curr_country == 'Non-EU' and prev_country == 'Non-EU':
                EUAs.append(0.0)

            elif curr_country == 'EU' and prev_country == 'EU' and curr_OMR == 'Yes' and prev_OMR == 'Yes':
                EUAs.append(0.0)

            else:
                EUAs.append(round(carbon_emitted * 0.7, 3))
    
        # ---------- Case 2: Voyage consumption ----------
        else:
            # (1) Current EU+No and Previous EU+No → Full 70%
            if curr_country == 'EU' and prev_country == 'EU' and curr_OMR == 'No' and prev_OMR == 'No':
                EUAs.append(round(carbon_emitted * 0.7, 3))

            elif curr_country == 'EU' and curr_OMR == 'No' and prev_OMR == 'Yes' and curr_country[:2] == prev_country[:2]:
                EUAs.append(0.0)
            
    
            # (2) Current EU+No OR Previous EU+No → 50%
            elif curr_country == 'EU' and curr_OMR == 'No' and prev_country == 'Non-EU' :
                EUAs.append(round(carbon_emitted * 0.7 * 0.5, 3))
            elif curr_country == 'Non-EU' and prev_country == 'EU' and prev_OMR == 'No':
                EUAs.append(round(carbon_emitted * 0.7 * 0.5, 3))
            elif curr_country == 'Non-EU' and prev_country == 'Non-EU':
                EUAs.append(0.0)
    
            # (3) OMR to OMR voyage
            elif curr_country == 'EU' and prev_country == 'EU' and curr_OMR == 'Yes' and prev_OMR == 'Yes':
                if curr_country[:2] == prev_country[:2]:
                    EUAs.append(0.0)
                else:
                    EUAs.append(round(carbon_emitted * 0.7, 3))
    
            # (4) Mixed EU–NonEU + OMR transitions → half rate
            elif (curr_country == 'EU' and prev_country == 'Non-EU' and curr_OMR == 'Yes') or (curr_country == 'Non-EU' and prev_country == 'EU' and prev_OMR == 'Yes'):
                EUAs.append(round(carbon_emitted * 0.7 * 0.5, 3))
    
            else:
                EUAs.append(0.0)

    # Allocating 'Category'
    boundary_type = []

    for i in range(len(filtered_df_2)):
    
        if i == 0:
            boundary_type.append("Start")
            continue
    
        prev_country = filtered_df_2.loc[i-1, "Country Code"]
        curr_country = filtered_df_2.loc[i, "Country Code"]
        curr_OMR = filtered_df_2.loc[i, 'OMR']
        prev_OMR = filtered_df_2.loc[i - 1, 'OMR']
    
        if prev_country == "EU" and curr_country == "Non-EU" and prev_OMR == "No":
            boundary_type.append("Outbound")

        elif prev_country == "EU" and curr_country == "Non-EU" and prev_OMR == "Yes":
            boundary_type.append("Outbound")
    
        elif prev_country == "Non-EU" and curr_country == "EU" and curr_OMR == "No":
            boundary_type.append("Inbound")

        elif prev_country == "Non-EU" and curr_country == "EU" and curr_OMR == "Yes":
            boundary_type.append("Inbound")
    
        elif prev_country == "EU" and curr_country == "EU" and prev_OMR == "No" and curr_OMR == "No":
            boundary_type.append("Bound")
    
        elif prev_country == "Non-EU" and curr_country == "Non-EU":
            boundary_type.append("Non-EU")

        elif prev_country == "EU" and prev_OMR == "Yes" and curr_country == "EU" and curr_OMR == "No":
            boundary_type.append("OMR-EU")

        elif prev_country == "EU" and prev_OMR == "No" and curr_country == "EU" and curr_OMR == "Yes":
            boundary_type.append("EU-OMR")

        elif prev_country == "Non-EU" and prev_OMR == "No" and curr_country == "EU" and curr_OMR == "Yes":
            boundary_type.append("NonEU-OMR")

        elif prev_country == "EU" and prev_OMR == "Yes" and curr_country == "Non-EU" and curr_OMR == "No":
            boundary_type.append("OMR-NonEU")

        elif prev_country == "EU" and prev_OMR == "Yes" and curr_country == "EU" and curr_OMR == "Yes":
            boundary_type.append("OMR")
        
        else:
            boundary_type.append("Unknown")

    filtered_df_2['EUAs'] = EUAs
    filtered_df_2["Category"] = boundary_type
    
    # Calculate summary metrics
    start_date = filtered_df_2['phase_end_date'].min()
    end_date = filtered_df_2['phase_end_date'].max()
    total_co2 = round(filtered_df_2['Carbon emitted'].sum(), 3)
    total_eua = round(filtered_df_2['EUAs'].sum(), 3)

    # Calculating individual & Total Energy & Applicable Energy
    half_applicable = {'Inbound', 'Outbound', 'EU-OMR', 'OMR-EU', 'NonEU-OMR', 'OMR-NonEU'}

    for fuel in ['hfo', 'lfo', 'mgo', 'lng']:
        con_col = f'cal_{fuel}_con'
        fueleu_col = f'cal_FuelEU_{fuel}_cons'
    
        con_values = filtered_df_2[con_col].fillna(0.0).astype('float64')
        
        filtered_df_2[fueleu_col] = np.where(
            filtered_df_2['Category'] == 'Bound',
            con_values,
            np.where(
                filtered_df_2['Category'].isin(half_applicable),
                con_values / 2.0,
                0.0
            )
        )
        # Round the column after assignment
        filtered_df_2[fueleu_col] = filtered_df_2[fueleu_col].round(3).astype('float64')
            
    filtered_df_2['Energy In Scope Without Reallocation'] = (
        filtered_df_2['cal_FuelEU_hfo_cons'].astype('float64') * 40500 + 
        filtered_df_2['cal_FuelEU_lfo_cons'].astype('float64') * 41000 + 
        filtered_df_2['cal_FuelEU_mgo_cons'].astype('float64') * 42700 + 
        filtered_df_2['cal_FuelEU_lng_cons'].astype('float64') * 49100
    )
    
    filtered_df_2['WtW CO2 In Scope Without Reallocation'] = (
        filtered_df_2['cal_FuelEU_hfo_cons'].astype('float64') * 3.71564 + 
        filtered_df_2['cal_FuelEU_lfo_cons'].astype('float64') * 3.74709 + 
        filtered_df_2['cal_FuelEU_mgo_cons'].astype('float64') * 3.87577 + 
        filtered_df_2['cal_FuelEU_lng_cons'].astype('float64') * 3.69113
    )
    
    filtered_df_2['Developing Energy In Scope Without Reallocation'] = filtered_df_2['Energy In Scope Without Reallocation'].cumsum()
    filtered_df_2['Developing WtW CO2 In Scope Without Reallocation'] = filtered_df_2['WtW CO2 In Scope Without Reallocation'].cumsum()

        
    num = filtered_df_2['Developing WtW CO2 In Scope Without Reallocation'].to_numpy(dtype='float64')
    den = filtered_df_2['Developing Energy In Scope Without Reallocation'].to_numpy(dtype='float64')
    
    # Prepare an output array initialised to NaN
    out = np.full_like(num, np.nan, dtype='float64')
    
    # Safe division: only where den != 0 the division is performed
    np.divide(num, den, out=out, where=den != 0)
    
    result1_without_reallocation = out * 10**6
    
    filtered_df_2['Developing Complience (GHGIE Actual) Without Reallocation'] = (
        pd.Series(result1_without_reallocation, index=filtered_df_2.index)
            .replace([np.inf, -np.inf], np.nan)
            .fillna(0.0)
            .astype('float64')
    )

    filtered_df_2['Developing (Compliance Balance) Without Reallocation'] = (
        (89.34 - filtered_df_2['Developing Complience (GHGIE Actual) Without Reallocation']) * 
        filtered_df_2['Developing Energy In Scope Without Reallocation']
    )

    # 2nd adjustment
    num2 = (filtered_df_2['Developing (Compliance Balance) Without Reallocation'] * 2400).to_numpy(dtype='float64')
    den2 = (filtered_df_2['Developing Complience (GHGIE Actual) Without Reallocation'] * 41000).to_numpy(dtype='float64')
    
    # Prepare output array initialised to NaN
    out2 = np.full_like(num2, np.nan, dtype='float64')
    
    # Safe division: only where den2 != 0
    np.divide(num2, den2, out=out2, where=den2 != 0)
    
    result2_without_reallocation = np.abs(out2)
    
    filtered_df_2['Developing (Fuel EU Penalty sum) Without Reallocation'] = (
        pd.Series(result2_without_reallocation, index=filtered_df_2.index)
            .replace([np.inf, -np.inf], np.nan)
            .fillna(0.0)
            .astype('float64')
    )

    cb_deficit_without_reallocation = filtered_df_2['Developing (Compliance Balance) Without Reallocation'].iloc[-1]
    penalty_without_reallocation = filtered_df_2['Developing (Fuel EU Penalty sum) Without Reallocation'].iloc[-1]

    filtered_df_2['HFO Energy'] = (filtered_df_2['cal_hfo_con'].fillna(0.0).astype('float64') * 40500).round(3)
    filtered_df_2['LFO Energy'] = (filtered_df_2['cal_lfo_con'].fillna(0.0).astype('float64') * 41000).round(3)
    filtered_df_2['MGO Energy'] = (filtered_df_2['cal_mgo_con'].fillna(0.0).astype('float64') * 42700).round(3)
    filtered_df_2['LNG Energy'] = (filtered_df_2['cal_lng_con'].fillna(0.0).astype('float64') * 49100).round(3)

    filtered_df_2['Total Energy'] = (
        filtered_df_2['cal_hfo_con'].fillna(0.0).astype('float64') * 40500 +
        filtered_df_2['cal_lfo_con'].fillna(0.0).astype('float64') * 41000 +
        filtered_df_2['cal_mgo_con'].fillna(0.0).astype('float64') * 42700 +
        filtered_df_2['cal_lng_con'].fillna(0.0).astype('float64') * 49100
    ).round(3)

    # Filtering based on Inbound/Outbound in case of Fuel EU with reallocation
    def get_applicable_energy(row):
        category = str(row['Category'])
    
        if category == 'Non-EU':
            return 0.0
        elif category == 'Bound':
            return float(row['Total Energy'])
        elif category in ['Inbound', 'Outbound', 'EU-OMR', 'OMR-EU']:
            return float(row['Total Energy']) / 2.0
        else:
            return 0.0

    filtered_df_2['Applicable Energy'] = filtered_df_2.apply(get_applicable_energy, axis=1).round(3)

    # Continue with remaining calculations (cargo calculations, etc.)

    return final_df, filtered_df_2, vessel_type

# --- Process and Display ---
if selected_vessel and from_date and to_date:
    with st.spinner('Processing vessel data...'):
        final_df, filtered_df, vessel_type = process_vessel_data(
            selected_vessel, from_date, to_date
        )
    
    if final_df is not None and filtered_df is not None:
        # Calculate summary
        start_date = filtered_df['phase_end_date'].min()
        end_date = filtered_df['phase_end_date'].max()
        total_co2 = round(filtered_df['Carbon emitted'].sum(), 3)
        total_eua = round(filtered_df['EUAs'].sum(), 3)
        
        # Summary Cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="info-card">
                <h3>Vessel</h3>
                <p>{selected_vessel}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="info-card">
                <h3>Total CO₂</h3>
                <p>{total_co2:,.1f} t</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="info-card">
                <h3>Total EUAs</h3>
                <p>{total_eua:,.1f}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="info-card">
                <h3>Vessel Type</h3>
                <p>{vessel_type if vessel_type else 'N/A'}</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Display Tables in Tabs
        tab1, tab2 = st.tabs(["📊 EUA Summary", "⚡ FuelEU Analysis"])
        
        with tab1:
            st.subheader("Vessel EUA Summary")
            
            display_columns = [
                'phase_end_date', 'phase', 'Country Code', 'OMR', 'port', 'cargo_mt',
                'hfo_rob', 'lfo_rob', 'mgo_rob', 'lng_rob',
                'hfo_bdn', 'lfo_bdn', 'mgo_bdn', 'lng_bdn',
                'cal_hfo_con', 'cal_lfo_con', 'cal_mgo_con', 'cal_lng_con',
                'Carbon emitted', 'EUAs'
            ]
            
            st.dataframe(
                filtered_df[display_columns],
                use_container_width=True,
                height=500
            )
            
            # Save to database button
            if st.button("Save Results to Database", type="primary", use_container_width=False):
                try:
                    engine = get_database_engine()
                    
                    # Save filtered data
                    filtered_df.to_sql(
                        "Cape_Ferrol_EUA_Summary",
                        engine,
                        if_exists="replace",
                        index=False
                    )
                    
                    engine.dispose()
                    
                    st.success("✅ Results saved to database successfully!")
                    
                except Exception as e:
                    st.error(f"❌ Error saving to database: {str(e)}")
        
        with tab2:
            st.subheader("FuelEU Maritime Analysis")
            st.info("Detailed FuelEU calculations and compliance analysis")
            st.dataframe(
                filtered_df[[
                    'phase_end_date', 'Category', 'Total Energy', 'Applicable Energy',
                    'Energy In Scope Without Reallocation', 
                    'Developing Complience (GHGIE Actual) Without Reallocation',
                    'Developing (Compliance Balance) Without Reallocation',
                    'Developing (Fuel EU Penalty sum) Without Reallocation'
                ]],
                use_container_width=True,
                height=500
            )
    else:
        st.warning("No data found for the selected vessel and date range.")
else:
    st.info("👈 Please select a vessel and date range from the sidebar to begin analysis.")
