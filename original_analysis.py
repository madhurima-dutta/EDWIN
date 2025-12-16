import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output, HTML
import calendar
import datetime
import sqlite3
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
import streamlit as st
import os
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL") or "postgresql://postgres.gyzsjokshqguccyfcbbi:avAsxDZpOMezTyo9@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
 
# --- SQLAlchemy engine (for pandas) ---
engine = create_engine(SUPABASE_DB_URL)
 
# --- Fetch tables into DataFrames ------------------------------------------------------------------------------------------------------------
df_vessel = pd.read_sql("SELECT * FROM vessel_reports", con=engine)
df_ports = pd.read_sql("SELECT * FROM \"Port_Name_List\"", con=engine)  # Quotes if camel case or uppercase
df_country = pd.read_sql("SELECT * FROM \"country_code_list\"", con=engine)
df_vessel_type = pd.read_sql("SELECT vessel_name, vessel_type FROM vessels_type_list", con=engine)

# Normalize
df_vessel_type['vessel_name'] = df_vessel_type['vessel_name'].str.strip()
df_vessel_type['vessel_type'] = df_vessel_type['vessel_type'].str.strip().str.upper()



#--------------------------------------------
#           MAIN LOGIC GOT STARTED
#--------------------------------------------


# Replace df_CapeFerrol with df_vessel
df_vessel['phase_end_date'] = pd.to_datetime(df_vessel['phase_end_date'])
df_vessel['date_str'] = df_vessel['phase_end_date'].dt.strftime('%Y-%m-%d %H:%M:%S')


#------------------------------------ Filling All Fast and Last Line ports-----------------------------------------------------------------

df_vessel.loc[
    (df_vessel['phase'].str.upper() == 'ALL FAST') & (df_vessel['start_port'].isna()),
    'start_port'
] = df_vessel.loc[
    (df_vessel['phase'].str.upper() == 'ALL FAST') & (df_vessel['start_port'].isna()),
    'end_port'
]


#------------------------------------Taking the first 5 letters from the port codes---------------------------------------------------------

df_vessel['start_port'] = df_vessel['start_port'].astype(str).str[:5]
df_vessel['end_port'] = df_vessel['end_port'].astype(str).str[:5]


# ----------------------------------- Normalize phase to uppercase and strip spaces --------------------------------------------------------

df_vessel['phase'] = df_vessel['phase'].str.strip().str.upper()

# ---------------------------- Fix cargo_mt consistency across voyage phases BEFORE filtering ----------------------------------------------

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




#----------------------------------------------------- Widget Setup ---------------------------------------------------------------------------

vessel_dropdown = widgets.Dropdown(
    options=sorted(df_vessel['vessel_name'].unique().tolist()),
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

#------------------------------------------- Function to enforce from-to date rule-----------------------------------------------------

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



#---------------------------------------------- Function to display filtered data--------------------------------------------------------

def display_filtered_data(*args):
    output_box.clear_output()
    with output_box:
        vessel = vessel_dropdown.value
        from_date = from_date_picker.value
        to_date = to_date_picker.value
        vessel_type_row = df_vessel_type[df_vessel_type['vessel_name'] == vessel]
        vessel_type = vessel_type_row['vessel_type'].values[0] if not vessel_type_row.empty else None
        

        if from_date and to_date:
            from_date_str = from_date.strftime('%Y-%m-%d')
            to_date_str = to_date.strftime('%Y-%m-%d')

            mask = (
                (df_vessel['vessel_name'] == vessel) &
                (df_vessel['phase_end_date'].dt.date >= from_date) &
                (df_vessel['phase_end_date'].dt.date <= to_date) &
                (df_vessel['phase'].isin(['ALL FAST', 'LAST LINE']))
            )

            final_df = df_vessel[mask].copy()
            final_df['port'] = final_df['start_port']
            final_df = final_df.sort_values(by='phase_end_date').reset_index(drop=True)



            final_df = final_df.drop_duplicates(subset=[
                'phase_end_date', 'phase', 'start_port', 'end_port',
                'hfo_rob', 'lfo_rob', 'mgo_rob', 'lng_rob',
                'hfo_bdn', 'lfo_bdn', 'mgo_bdn', 'lng_bdn'
            ], keep='first').reset_index(drop=True)
            

                
#---------------------------------Placing the Bunker values properly (For Display 1) ------------------------------------------------------------
            
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
                    ][bdn_col].sum(min_count=1)  # min_count=1 ensures NaN if nothing found
            
                    if pd.isna(bdn_sum):
                        bdn_sum = 0  # default if no BDN found
            
                    # Update BDN for current row
                    final_df.at[i, bdn_col] = bdn_sum
            
                    # Calculate consumption
                    final_df.at[i, calc_col] = (
                        final_df.at[i-1, rob_col] + 
                        final_df.at[i, bdn_col] - 
                        final_df.at[i, rob_col]
                    )

#------------------------------------------------------------------------------------------------------------------------------------------------
#                                PRINTING DISPLAY 1 FOR ALL KIND OF VESSELS
#------------------------------------------------------------------------------------------------------------------------------------------------
            

            print(f"\n Display 1: All 'ALL FAST' & 'LAST LINE' Data from {from_date_str} to {to_date_str} for '{vessel}':")
            display(final_df[[
                'phase_end_date', 'phase', 'port', 'cargo_mt',
                'hfo_rob', 'lfo_rob', 'mgo_rob', 'lng_rob',
                'hfo_bdn', 'lfo_bdn', 'mgo_bdn', 'lng_bdn',
                'cal_hfo_con', 'cal_lfo_con', 'cal_mgo_con', 'cal_lng_con'
            ]])



            # Remove AF–LL duplicates

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


                    

#---------------------------------------------------------------------------------------------------------------------------------------------------
            

            

            print(f"\n Display 2: Cargo-Changing Legs Only (Excludes same cargo_mt AF–LL pairs):")
            display(filtered_df_2[[
                'phase_end_date', 'phase', 'Country Code', 'OMR', 'Category', 'port', 'cargo_mt',
                'hfo_rob', 'lfo_rob', 'mgo_rob', 'lng_rob',
                'hfo_bdn', 'lfo_bdn', 'mgo_bdn', 'lng_bdn',
                'cal_hfo_con', 'cal_lfo_con', 'cal_mgo_con', 'cal_lng_con',
                'Carbon emitted', 'EUAs'
            ]])     #removed 'cal_FuelEU_hfo_con','cal_FuelEU_lfo_con', 'cal_FuelEU_mgo_con', 'cal_FuelEU_lng_con' for spacing issue

            
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


                

                # Create a DataFrame for the second summary
                table_4 = pd.DataFrame({
                    'CB_Def Without Fuel Re-allocation': [f"{cb_deficit_without_reallocation:.3f}"],
                    'Penalty (EUR) Without Fuel Re-allocation': [round(penalty_without_reallocation, 3)],
                    'CB_Def With Fuel Re-allocation': [f"{cb_deficit_with_reallocation:.3f}"],
                    'Penalty (EUR) With Fuel Re-allocation' : [round(penalty_with_reallocation, 3)] 
                })

                display(HTML(f"""
                <div style="background-color:#cce5ff; padding:10px; border-radius:5px; border:1px solid #99ccff;">
                <b>FuelEU Calculation
                """))
               
                # Use same CSS styling
                styled_html_2 = table_4.to_html(index=False, border=1, justify='center', classes='styled-table')
                display(HTML(css + styled_html_2))
                

                # Take the first row from table_4 and repeat to match filtered_df_2 length
                row_2nd_repeated = pd.DataFrame([table_4.iloc[0]] * len(filtered_df_2)).reset_index(drop=True)
                
                # Merge both DataFrames side-by-side
                merged_df = pd.concat([filtered_df_2.reset_index(drop=True), row_2nd_repeated], axis=1)
                

            else:
                print("\nNo data available for summary table.")
        else:
            print("Please select both From Date and To Date (To Date must be on/after From Date).")

# Attach widget events
vessel_dropdown.observe(display_filtered_data, names='value')
from_date_picker.observe(enforce_to_date_limit, names='value')
to_date_picker.observe(display_filtered_data, names='value')

display(widgets.VBox([
    vessel_dropdown,
    widgets.HBox([from_date_picker, to_date_picker]),
    output_box
]))
