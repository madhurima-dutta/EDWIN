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
        z-index: 1;
        letter-spacing: -0.5px;
    }
    
    .main-header p {
        margin: 0.5rem 0 0 0;
        font-size: 1rem;
        opacity: 0.95;
        position: relative;
        z-index: 1;
        font-weight: 300;
    }
    
    /* ===== METRIC CARDS ===== */
    .metric-card {
        background: var(--card-bg);
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
        border: 1px solid var(--border-color);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
    }
    
    .metric-card:hover {
        box-shadow: 0 8px 24px rgba(0, 82, 204, 0.25);
        transform: translateY(-2px);
        border-color: var(--primary-light);
    }
    
    .metric-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, var(--primary-color), var(--secondary-color));
    }
    
    .metric-card.co2::before {
        background: linear-gradient(90deg, #ff6b35, #ff8c5a);
    }
    
    .metric-card.eua::before {
        background: linear-gradient(90deg, #0052cc, #0066ff);
    }
    
    .metric-card.ghg::before {
        background: linear-gradient(90deg, #00d084, #00a366);
    }
    
    .metric-card.penalty::before {
        background: linear-gradient(90deg, #00d4ff, #00a8cc);
    }
    
    .metric-label {
        font-size: 0.75rem;
        color: var(--text-secondary);
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin-bottom: 0.5rem;
        display: block;
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: 800;
        color: var(--text-primary);
        margin: 0.25rem 0;
        letter-spacing: -0.5px;
    }
    
    .metric-unit {
        font-size: 0.85rem;
        color: var(--text-secondary);
        font-weight: 500;
    }
    
    /* ===== SECTION HEADERS ===== */
    .section-header {
        font-size: 1.4rem;
        font-weight: 800;
        color: var(--text-primary);
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.75rem;
        border-bottom: 2px solid var(--primary-color);
        display: inline-block;
        letter-spacing: -0.3px;
    }
    
    /* ===== SIDEBAR STYLING ===== */
    .sidebar-section {
        background: linear-gradient(135deg, #1a1f3a 0%, #151a2f 100%);
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        border-left: 3px solid var(--primary-color);
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
    }
    
    /* ===== DATAFRAME STYLING ===== */
    .dataframe-container {
        background: var(--card-bg);
        border-radius: 12px;
        padding: 1.25rem;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
        border: 1px solid var(--border-color);
        overflow-x: auto;
        margin: 1rem 0;
    }
    
    /* ===== SUMMARY TABLE ===== */
    .summary-table {
        background: var(--card-bg);
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
        border: 1px solid var(--border-color);
    }
    
    /* ===== BUTTON STYLING ===== */
    .stButton > button {
        background: linear-gradient(135deg, #0052cc 0%, #0066ff 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 2rem;
        font-weight: 700;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 4px 16px rgba(0, 82, 204, 0.3);
        font-size: 0.95rem;
        letter-spacing: 0.2px;
    }
    
    .stButton > button:hover {
        box-shadow: 0 8px 24px rgba(0, 82, 204, 0.4);
        transform: translateY(-1px);
    }
    
    /* ===== FOOTER ===== */
    .footer {
        text-align: center;
        color: var(--text-secondary);
        font-size: 0.85rem;
        margin-top: 3rem;
        padding-top: 1.5rem;
        border-top: 1px solid var(--border-color);
        font-weight: 500;
    }
    
    /* ===== RESPONSIVE DESIGN ===== */
    @media (max-width: 768px) {
        .main-header h1 {
            font-size: 1.8rem;
        }
        
        .metric-value {
            font-size: 1.5rem;
        }
        
        .section-header {
            font-size: 1.1rem;
        }
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
    """Load data from database with caching"""
    try:
        engine = create_engine(SUPABASE_DB_URL)
        
        # Fetch tables into DataFrames
        df_vessel = pd.read_sql("SELECT * FROM vessel_reports", con=engine)
        df_ports = pd.read_sql("SELECT * FROM \"Port_Name_List\"", con=engine)
        df_country = pd.read_sql("SELECT * FROM \"country_code_list\"", con=engine)
        df_vessel_type = pd.read_sql("SELECT vessel_name, vessel_type FROM vessels_type_list", con=engine)

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
        return None, None, None, None


def get_database_engine():
    """Create database engine for operations"""
    return create_engine(SUPABASE_DB_URL)

# Load data
with st.spinner("Loading vessel data..."):
    df_vessel, df_ports, df_country, df_vessel_type = load_data()

if df_vessel is not None:
    st.session_state.data_loaded = True
    
    # --- Sidebar Controls ---
    st.sidebar.markdown("### 📊 Filter Options")
    
    # Vessel selection
    vessel_options = sorted(df_vessel['vessel_name'].unique().tolist())
    selected_vessel = st.sidebar.selectbox(
        "Select Vessel:",
        vessel_options,
        index=0 if vessel_options else None
    )

    # --- Backend-only vessel type lookup ---
    vessel_type_row = df_vessel_type.loc[
        df_vessel_type['vessel_name'] == selected_vessel, 
        'vessel_type'
    ]

    vessel_type = vessel_type_row.iloc[0] if not vessel_type_row.empty else None

    
    # Date selection
    st.sidebar.markdown("### 📅 Date Range")
    
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
        min_value=from_date,
        max_value=max_date
    )
    
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
            
            # initialize consumption
            final_df[calc_col] = None  
            
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
                    bdn_sum = 0
            
                # Update BDN for current row
                final_df.at[i, bdn_col] = bdn_sum
            
                # Calculate consumption
                final_df.at[i, calc_col] = (
                    final_df.at[i-1, rob_col] + 
                    final_df.at[i, bdn_col] - 
                    final_df.at[i, rob_col]
                )
        
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



#-----------------------------Display 1 making completed and going forward to make display 2 -----------------------------------------------------
            
        filtered_df_2 = final_df.drop(index=rows_to_remove).reset_index(drop=True)
            



#---------------------------------Placing the Bunker values properly (For Display 2) -----------------------------------------------------------

        for fuel in ['hfo', 'lfo', 'mgo', 'lng']:
            rob_col = f'{fuel}_rob'
            bdn_col = f'{fuel}_bdn'
            calc_col = f'cal_{fuel}_con'

            # initialize consumption
            filtered_df_2[calc_col] = None  
        
            for i in range(1, len(filtered_df_2)):
                prev_date = filtered_df_2.at[i-1, 'phase_end_date']
                curr_date = filtered_df_2.at[i, 'phase_end_date']
        
                # Sum BDN values between (prev_date, curr_date]
                bdn_sum = df_vessel[
                    (df_vessel['vessel_name'] == vessel) &
                    (df_vessel['phase_end_date'] > prev_date) &
                    (df_vessel['phase_end_date'] <= curr_date)
                ][bdn_col].sum(min_count=1)  # min_count=1 ensures NaN if nothing found
        
                if pd.isna(bdn_sum):
                    bdn_sum = 0  # default if no BDN found
        
                # Update BDN for current row
                filtered_df_2.at[i, bdn_col] = bdn_sum
        
                # Calculate consumption
                filtered_df_2.at[i, calc_col] = (
                    filtered_df_2.at[i-1, rob_col] + 
                    filtered_df_2.at[i, bdn_col] - 
                    filtered_df_2.at[i, rob_col]
                )

#------------------------------------------------------------------------------------------------------------------------------------------------
        port_to_country = df_ports.set_index('Port Code')['EU Ports'].to_dict()
        port_to_omr = df_ports.set_index('Port Code')['OMR'].to_dict()

        filtered_df_2['Country Code'] = filtered_df_2['port'].map(port_to_country)
        filtered_df_2['OMR'] = filtered_df_2['port'].map(port_to_omr)
        
        filtered_df_2['Carbon emitted'] = (
            filtered_df_2['cal_hfo_con'].fillna(0) * 3.114 +
            filtered_df_2['cal_lfo_con'].fillna(0) * 3.151 +
            filtered_df_2['cal_mgo_con'].fillna(0) * 3.206 +
            filtered_df_2['cal_lng_con'].fillna(0) * 2.75
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
            carbon_emitted = filtered_df_2.loc[i, 'Carbon emitted']
        
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

                elif curr_country == 'EU' and curr_OMR == 'No' and prev_OMR == 'Yes' and curr_country[:2] == prev_country[:2]: #OMR to Mainland
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


#------------------------------------------- Allocating 'Category' ----------------------------------------------------------------------------------            

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

#----------------------------------- Calculating individual & Total Energy & Applicable Energy ----------------------------------------------------
            
        half_applicable = {'Inbound', 'Outbound', 'EU-OMR', 'OMR-EU', 'NonEU-OMR', 'OMR-NonEU'}

        for fuel in ['hfo', 'lfo', 'mgo', 'lng']:
            con_col = f'cal_{fuel}_con'
            fueleu_col = f'cal_FuelEU_{fuel}_cons'
        
            filtered_df_2[fueleu_col] = np.where(
                filtered_df_2['Category'] == 'Bound',
                filtered_df_2[con_col],
                np.where(
                    filtered_df_2['Category'].isin(half_applicable),
                    filtered_df_2[con_col] / 2,
                    0
                )
            )
            # Round the column after assignment
            filtered_df_2[fueleu_col] = filtered_df_2[fueleu_col].round(3)
                
        filtered_df_2['Energy In Scope Without Reallocation'] = filtered_df_2['cal_FuelEU_hfo_cons']*40500 + filtered_df_2['cal_FuelEU_lfo_cons']*41000 + filtered_df_2['cal_FuelEU_mgo_cons']*42700 + filtered_df_2['cal_FuelEU_lng_cons']*49100
        filtered_df_2['WtW CO2 In Scope Without Reallocation'] = filtered_df_2['cal_FuelEU_hfo_cons']*3.71564 + filtered_df_2['cal_FuelEU_lfo_cons']*3.74709 + filtered_df_2['cal_FuelEU_mgo_cons']*3.87577 + filtered_df_2['cal_FuelEU_lng_cons']*3.69113
        filtered_df_2['Developing Energy In Scope Without Reallocation'] = filtered_df_2['Energy In Scope Without Reallocation'].cumsum()
        filtered_df_2['Developing WtW CO2 In Scope Without Reallocation'] = filtered_df_2['WtW CO2 In Scope Without Reallocation'].cumsum()

            
        num = filtered_df_2['Developing WtW CO2 In Scope Without Reallocation'].to_numpy(dtype='float64')
        den = filtered_df_2['Developing Energy In Scope Without Reallocation'].to_numpy(dtype='float64')
        
        # Prepare an output array initialised to NaN (or 0 if you prefer)
        out = np.full_like(num, np.nan, dtype='float64')
        
        # Safe division: only where den != 0 the division is performed
        np.divide(num, den, out=out, where=den != 0)
        
        result1_without_reallocation = out * 10**6
        
        filtered_df_2['Developing Complience (GHGIE Actual) Without Reallocation'] = (
            pd.Series(result1_without_reallocation, index=filtered_df_2.index)
                .replace([np.inf, -np.inf], np.nan)
                .fillna(0)
        )


        filtered_df_2['Developing (Compliance Balance) Without Reallocation'] = (89.34 - filtered_df_2['Developing Complience (GHGIE Actual) Without Reallocation']) * filtered_df_2['Developing Energy In Scope Without Reallocation']


        #-------------- 2nd adjustment ------------------------
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
                .fillna(0)
        )


        
        cb_deficit_without_reallocation = filtered_df_2['Developing (Compliance Balance) Without Reallocation'].iloc[-1]
        penalty_without_reallocation = filtered_df_2['Developing (Fuel EU Penalty sum) Without Reallocation'].iloc[-1]
            

            

#-----------------------------------------------------------------------------------------------------------------------------------
            
        filtered_df_2['HFO Energy'] = (filtered_df_2['cal_hfo_con'].fillna(0) * 40500).round(3)
        filtered_df_2['LFO Energy'] = (filtered_df_2['cal_lfo_con'].fillna(0) * 41000).round(3)
        filtered_df_2['MGO Energy'] = (filtered_df_2['cal_mgo_con'].fillna(0) * 42700).round(3)
        filtered_df_2['LNG Energy'] = (filtered_df_2['cal_lng_con'].fillna(0) * 49100).round(3)

        filtered_df_2['Total Energy'] = (
            filtered_df_2['cal_hfo_con'].fillna(0) * 40500 +
            filtered_df_2['cal_lfo_con'].fillna(0) * 41000 +
            filtered_df_2['cal_mgo_con'].fillna(0) * 42700 +
            filtered_df_2['cal_lng_con'].fillna(0) * 49100
        ).round(3)

        #--------------------------- Filtering based on Inbound/Outbound in case of Fuel EU with reallocation ------------------------------
        def get_applicable_energy(row):
            category = str(row['Category'])
        
            if category == 'Non-EU':
                return 0
            elif category == 'Bound':
                return row['Total Energy']
            elif category in ['Inbound', 'Outbound', 'EU-OMR', 'OMR-EU']:
                return row['Total Energy'] / 2
            else:
                return 0   # default if category is something unexpected

        
        filtered_df_2['Applicable Energy'] = filtered_df_2.apply(get_applicable_energy, axis=1).round(3)



#--------------------------------------- Fuel EU efficient Energy allocation hierarchy-----------------------------------------------------------
#----------------------------------------------- With Reallocation of Fuel ----------------------------------------------------------------------

            

        filtered_df_2['LNG Efficient Energy'] = np.minimum(filtered_df_2['Applicable Energy'], filtered_df_2['LNG Energy'])
        filtered_df_2['MGO Efficient Energy'] = np.maximum(0, np.minimum(filtered_df_2['Applicable Energy'] - filtered_df_2['LNG Efficient Energy'], filtered_df_2['MGO Energy']))
        filtered_df_2['LFO Efficient Energy'] = np.maximum(0, np.minimum(filtered_df_2['Applicable Energy'] - (filtered_df_2['LNG Efficient Energy'] + filtered_df_2['MGO Efficient Energy']) , filtered_df_2['LFO Energy']))
        filtered_df_2['HFO Efficient Energy'] = np.maximum(0, np.minimum(filtered_df_2['Applicable Energy'] - (filtered_df_2['LNG Efficient Energy'] + filtered_df_2['MGO Efficient Energy'] + filtered_df_2['LFO Efficient Energy']) , filtered_df_2['HFO Energy']))

        filtered_df_2['Total WtW CO2 In Scope'] = ((filtered_df_2['LNG Efficient Energy'] / 49100) * 3.69113) + ((filtered_df_2['MGO Efficient Energy'] / 42700) * 3.87577) + ((filtered_df_2['LFO Efficient Energy'] / 41000) * 3.74709) + ((filtered_df_2['HFO Efficient Energy'] / 40500) * 3.71564)
        
        
        num = (
            filtered_df_2['LNG Efficient Energy'] * 75.176 +
            filtered_df_2['MGO Efficient Energy'] * 90.767 +
            filtered_df_2['LFO Efficient Energy'] * 91.392 +
            filtered_df_2['HFO Efficient Energy'] * 91.744
        )


        # Calculate safely
        result = num / filtered_df_2['Applicable Energy']
        
        
        # Replace errors (NaN, inf) and zeros with 89.34
        filtered_df_2['GHGIE Actual44'] = result.replace([np.inf, -np.inf], np.nan)
        filtered_df_2['GHGIE Actual44'] = filtered_df_2['GHGIE Actual44'].fillna(89.34)
        filtered_df_2['GHGIE Actual44'] = np.where(filtered_df_2['GHGIE Actual44'] == 0, 89.34, filtered_df_2['GHGIE Actual44'])


        filtered_df_2['Complience Balance'] = (89.34 - filtered_df_2['GHGIE Actual44'] ) * filtered_df_2['Applicable Energy']
        

        filtered_df_2['FuelEU Penalty'] = abs((filtered_df_2['Complience Balance'] * 2400)/(filtered_df_2['GHGIE Actual44'] * 41000))
        filtered_df_2['Developing Compliance (Energy In Scope)'] = filtered_df_2['Applicable Energy'].cumsum()
        filtered_df_2['Developing Compliance (WtW In Scope)'] = filtered_df_2['Total WtW CO2 In Scope'].cumsum()
        
        result1 = (filtered_df_2['Developing Compliance (WtW In Scope)'] / filtered_df_2['Developing Compliance (Energy In Scope)']) * 10**6

        
        # Replace errors (division by zero, NaN, inf) with 0
        filtered_df_2['Developing Compliance (GHGIE Actual)'] = (
            result1.replace([np.inf, -np.inf], np.nan)
                    .fillna(0)
        )


        filtered_df_2['Developing (Compliance Balance)42'] = (89.34 - filtered_df_2['Developing Compliance (GHGIE Actual)']) * filtered_df_2['Developing Compliance (Energy In Scope)']
        

        result2 = abs((filtered_df_2['Developing (Compliance Balance)42'] * 2400)/(filtered_df_2['Developing Compliance (GHGIE Actual)'] * 41000))
        filtered_df_2['Developing Compliance (Fuel EU Penalty Sum Value)'] = (
            result2.replace([np.inf, -np.inf], np.nan)
                    .fillna(0)
        )



        penalty_with_reallocation = filtered_df_2['Developing Compliance (Fuel EU Penalty Sum Value)'].iloc[-1]
        cb_deficit_with_reallocation = filtered_df_2['Developing (Compliance Balance)42'].iloc[-1]

    

            
        hierarchy_df = pd.DataFrame([
                [1, "LPG (Propane)", 49100, 73.017, 0],
                [2, "LPG (Butane)", 49100, 73.670, 0],
                [3, "LNG (Boiler)", 49100, 75.176, 3.69113],
                [4, "LNG (Diesel Slow)", 49100, 76.081, 3.73556444],
                [5, "LNG (Otto Slow)", 49100, 82.868, 4.06882274],
                [6, "LNG (LBSI)", 49100, 86.940, 4.26877772],
                [7, "LNG (Otto Medium)", 49100, 89.203, 4.37986382],
                [8, "MGO", 42700, 90.767, 3.87577],
                [9, "LFO", 41000, 91.392, 3.74709],
                [10, "HFO", 40500, 91.744, 3.71564],
                [11, "Methanol", 0, 100.395, 0]
            ], columns=["Hierarchy", "Fuel Type", "LCV", "WtW GHIGE", "CO2 Eq WtW"]) #This will be used for "With reallocation" case 
        
        # Create summary tables
        eua_summary = pd.DataFrame({
            'From': [start_date.strftime('%Y-%m-%d %H:%M:%S')],
            'To': [end_date.strftime('%Y-%m-%d %H:%M:%S')],
            'CO₂ Emitted (mt)': [total_co2],
            'EUAs': [total_eua]
        })
        
        fueleu_summary = pd.DataFrame({
                    'CB_Def Without Fuel Re-allocation': [f"{cb_deficit_without_reallocation:.3f}"],
                    'Penalty (EUR) Without Fuel Re-allocation': [round(penalty_without_reallocation, 3)],
                    'CB_Def With Fuel Re-allocation': [f"{cb_deficit_with_reallocation:.3f}"],
                    'Penalty (EUR) With Fuel Re-allocation' : [round(penalty_with_reallocation, 3)] 
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
                st.markdown("<h2 class='section-header'>📊 Key Performance Indicators</h2>", unsafe_allow_html=True)
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.markdown(f"""
                    <div class="metric-card co2">
                        <span class="metric-label">CO₂ Emissions</span>
                        <div class="metric-value">{eua_summary['CO₂ Emitted (mt)'].iloc[0]}</div>
                        <span class="metric-unit">metric tonnes</span>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"""
                    <div class="metric-card eua">
                        <span class="metric-label">EUAs Required</span>
                        <div class="metric-value">{eua_summary['EUAs'].iloc[0]}</div>
                        <span class="metric-unit">allowances</span>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"""
                    <div class="metric-card ghg">
                        <span class="metric-label">GHG Intensity</span>
                        <div class="metric-value">{fueleu_summary['CB_Def Without Fuel Re-allocation'].iloc[0]}</div>
                        <span class="metric-unit">g CO₂eq/MJ</span>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col4:
                    penalty_value = fueleu_summary['Penalty (EUR) Without Fuel Re-allocation'].iloc[0]
                    st.markdown(f"""
                    <div class="metric-card penalty">
                        <span class="metric-label">FuelEU Penalty</span>
                        <div class="metric-value">€{fueleu_summary['Penalty (EUR) Without Fuel Re-allocation'].iloc[0]}</div>
                        <span class="metric-unit">EUR</span>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("<h2 class='section-header'>📋 Detailed Voyage Data</h2>", unsafe_allow_html=True)
                
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
                    hide_index=True
                )
                
                st.markdown("<h2 class='section-header'>📊 Summary Analysis</h2>", unsafe_allow_html=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("<h3 style='font-size: 1.1rem; color: var(--text-primary); margin: 0 0 1rem 0;'>🌍 EUA Calculation</h3>", unsafe_allow_html=True)
                    st.dataframe(eua_summary, use_container_width=True, hide_index=True)
                
                with col2:
                    st.markdown("<h3 style='font-size: 1.1rem; color: var(--text-primary); margin: 0 0 1rem 0;'>⛽ FuelEU Maritime</h3>", unsafe_allow_html=True)
                    st.dataframe(fueleu_summary, use_container_width=True, hide_index=True)
                
                st.markdown("<h2 class='section-header'>💾 Export Results</h2>", unsafe_allow_html=True)
                
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
st.markdown("""
<div class='footer'>
    🚢 Vessel EUAs & FuelEU Maritime Penalty Calculator | Built with Streamlit
</div>
""", unsafe_allow_html=True)
