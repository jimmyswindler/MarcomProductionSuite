# 10_DataCollection.py
# --- Dependencies and Setup ---
import pandas as pd
import numpy as np
import datetime
import os
import shutil
import traceback
import glob 
import sys
import json               
import argparse           
from typing import List, Dict, Tuple, Any
from datetime import datetime, timedelta, date

import utils_ui # <--- New UI Utility

# --- Holiday Libraries ---
try:
    import holidays
    from holidays.countries import UnitedStates
except ImportError:
    utils_ui.print_error("Fatal Error: Required library 'holidays' not found.")
    print("Please run: pip install holidays")
    sys.exit(1)

# --- Ship Date Calculation Helpers ---
class CustomUS(UnitedStates):
    def _populate(self, year):
        super()._populate(year)
        thanksgiving_date = None
        for date_obj, name in self.items():
            if name == "Thanksgiving":
                thanksgiving_date = date_obj
                break
        if thanksgiving_date:
            self[thanksgiving_date + timedelta(days=1)] = "Day after Thanksgiving"

def calculate_ship_date(order_date, lead_time_days=5):
    if pd.isna(order_date): return pd.NaT
    current_date = None
    if isinstance(order_date, datetime): current_date = order_date.date()
    else:
        try: current_date = pd.Timestamp(order_date).date()
        except Exception: return pd.NaT
    if current_date is None: return pd.NaT
    us_holidays = CustomUS(observed=True, years=current_date.year)
    ship_date_calc = current_date + timedelta(days=lead_time_days)
    while ship_date_calc.weekday() >= 5 or ship_date_calc in us_holidays:
        ship_date_calc += timedelta(days=1)
        current_holiday_years = getattr(us_holidays, '_years', getattr(us_holidays, 'years', [0]))
        if isinstance(current_holiday_years, list): current_holiday_years = set(current_holiday_years)
        if ship_date_calc.year not in current_holiday_years:
             us_holidays = CustomUS(observed=True, years=ship_date_calc.year)
    return pd.Timestamp(ship_date_calc)


# --- Column Order Configuration ---
PREFERRED_COLUMN_ORDER = [
    'job_ticket_number', 'product_id', 'quantity_ordered', 'order_number', 'order_item_id',
    'order_date', 'ship_date',
    'cost_center', 'sku', 'ship_to_name', 'ship_attn', 'ship_to_company',
    'address1', 'address2', 'address3', 'address4', 'city', 'state', 'zip', 'country',
    'special_instructions', 'product_name', 'general_description', 'paper_description',
    'press_instructions', 'bindery_instructions', 'job_ticket_shipping_instructions',
    'sku_description', 'product_description', '1-up_output_file_url'
]

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes all column names in a DataFrame."""
    new_cols = []
    for col in df.columns:
        new_col = str(col).lower().strip().replace(' ', '_').replace('#', 'num')
        new_cols.append(new_col)
    df.columns = new_cols
    return df

def load_data(file_paths_map: Dict[str, str]) -> Tuple[Dict[str, pd.DataFrame], Dict[str, str]]:
    """Loads source XLSX files from the exact paths provided."""
    dataframes = {}
    utils_ui.print_info("Loading source files...") 

    for key, path in file_paths_map.items():
        if not os.path.exists(path):
            raise FileNotFoundError(f"Error: Provided file for '{key}' not found at: {path}")

        filename = os.path.basename(path)
        try:
            df = pd.read_excel(path, engine='openpyxl')
        except Exception as e:
            utils_ui.print_error(f"Failed to read Excel file {filename}: {e}")
            raise 

        df = clean_column_names(df)
        dataframes[key] = df
        utils_ui.print_info(f"Loaded: {int(len(df))} rows from {filename}")

    return dataframes, file_paths_map


def preprocess_data(dataframes: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Ensures key columns have consistent data types for merging."""
    for df in dataframes.values():
        if 'job_ticket_number' in df.columns:
            df['job_ticket_number'] = df['job_ticket_number'].astype(str)
        if 'sku' in df.columns:
            df['sku'] = df['sku'].astype(str)
        if 'order_item_id' in df.columns:
            df['order_item_id'] = pd.to_numeric(df['order_item_id'], errors='coerce').astype('Int64')
    # utils_ui.print_info("Pre-processing complete: Normalized data types.") 
    return dataframes

def merge_data(dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Executes the sequential merge strategy."""
    df_primary_1 = dataframes['job_ticket'].copy()
    df_secondary_1 = dataframes['order_ship'].copy()
    df_primary_1['tie_breaker'] = df_primary_1.groupby(['job_ticket_number', 'sku']).cumcount()
    df_secondary_1['tie_breaker'] = df_secondary_1.groupby(['job_ticket_number', 'sku']).cumcount()
    
    merged_df_1 = pd.merge(df_primary_1, df_secondary_1, on=['job_ticket_number', 'sku', 'tie_breaker'], how='outer', suffixes=('', f"_from_order_ship"))
    
    df_secondary_2 = dataframes['total_order'].copy()
    final_df = pd.merge(merged_df_1, df_secondary_2, on='order_item_id', how='outer', suffixes=('', f"_from_total_order"))
    
    if 'tie_breaker' in final_df.columns:
        final_df = final_df.drop(columns=['tie_breaker'])
    
    utils_ui.print_success("Data Merge Complete (JobTicket + OrderShip + TotalOrder)")
    return final_df

def finalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Reorders columns and sorts the final DataFrame."""
    existing_cols = [col for col in PREFERRED_COLUMN_ORDER if col in df.columns]
    other_cols = sorted([col for col in df.columns if col not in existing_cols])
    df = df[existing_cols + other_cols]
    if 'job_ticket_number' in df.columns and 'sku' in df.columns:
        df = df.sort_values(by=['job_ticket_number', 'sku'], ascending=True)
    return df

def generate_box_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Creates the eight new box_X columns."""
    if 'order_item_id' not in df.columns:
        utils_ui.print_warning("'order_item_id' column not found. Skipping box column generation.")
        return df
        
    order_item_str = df['order_item_id'].astype(str).str.replace('<NA>', '').str.replace('nan', '')
    
    box_cols_and_suffixes = {
        'box_A': 'A', 'box_B': 'B', 'box_C': 'C', 'box_D': 'D',
        'box_E': 'E', 'box_F': 'F', 'box_G': 'G', 'box_H': 'H'
    }
    
    for col_name, suffix in box_cols_and_suffixes.items():
        df[col_name] = order_item_str + suffix
        
    return df

def clean_dataframe_for_output(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the final DataFrame for output."""
    df_clean = df.copy()
    object_cols = df_clean.select_dtypes(include=['object']).columns
    df_clean[object_cols] = df_clean[object_cols].fillna('')
    return df_clean

def generate_and_log_summary(source_files: Dict[str, str], final_report_df: pd.DataFrame, file_path: str, success: bool = True, error_details: str = "") -> None:
    if success:
        msg = f"Consalidation Summary:\n"
        msg += f"  - Final Report: {os.path.basename(file_path)}\n"
        msg += f"  - Total Rows: {len(final_report_df)}\n"
        msg += f"  - Total Columns: {len(final_report_df.columns)}"
        utils_ui.print_success(msg)
    else:
        utils_ui.print_error(f"Consolidation Failed: {error_details}")

# --- Main Function ---
def main(staging_dir: str, file_paths_map: Dict[str, str]) -> None:
    """Main function to execute the entire data consolidation pipeline."""
    utils_ui.setup_logging(None) # Use passed config if we had it, but here we just stream to stdout
    utils_ui.print_banner("10 - Data Collection")

    source_files, final_report_df = {}, pd.DataFrame()

    try:
        os.makedirs(staging_dir, exist_ok=True)
        
        # 1. Load & Process
        all_data, source_files = load_data(file_paths_map)
        all_data = preprocess_data(all_data)
        merged_df = merge_data(all_data)
        final_report_df = finalize_dataframe(merged_df)
        final_report_df = generate_box_columns(final_report_df)

        # 2. Dynamic file naming
        output_file_name = f'Consolidated_Report_{datetime.now().strftime("%Y-%m-%d")}.xlsx'
        
        if 'order_date' in final_report_df.columns:
            final_report_df['order_date'] = pd.to_datetime(final_report_df['order_date'], errors='coerce')
            
            utils_ui.print_info("Calculating ship dates...")
            final_report_df['ship_date'] = final_report_df['order_date'].apply(calculate_ship_date)
            
            order_dates = final_report_df['order_date'].dt.date.dropna()
            if not order_dates.empty:
                min_date, max_date = order_dates.min(), order_dates.max()
                min_date_str, max_date_str = min_date.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d')
                output_file_name = f'MarcomOrderDate {min_date_str}.xlsx' if min_date == max_date else f'MarcomOrderDate {min_date_str}_to_{max_date_str}.xlsx'

        output_file_path = os.path.join(staging_dir, output_file_name)

        # 3. Save
        cleaned_report_df = clean_dataframe_for_output(final_report_df)
        cleaned_report_df.to_excel(output_file_path, index=False, engine='openpyxl')
        utils_ui.print_success(f"Created Report: {os.path.basename(output_file_path)}")

        # 4. Move Source Files
        utils_ui.print_info("Moving source files to staging...")
        for source_key, source_path in source_files.items(): 
             if os.path.exists(source_path):
                 filename = os.path.basename(source_path)
                 dest_path = os.path.join(staging_dir, filename) 
                 try:
                     shutil.move(source_path, dest_path)
                 except Exception as move_err:
                     utils_ui.print_warning(f"Failed to move '{filename}': {move_err}")

        generate_and_log_summary(source_files, final_report_df, output_file_path, success=True)
        utils_ui.print_success("Stage 1 Complete.")

    except (FileNotFoundError, FileExistsError) as e:
        error_details = str(e)
        utils_ui.print_error(f"File Error: {error_details}")
        generate_and_log_summary({}, pd.DataFrame(), "", success=False, error_details=error_details)
        sys.exit(1) 
    except Exception as e:
        error_details = f"Unexpected Error: {e}"
        utils_ui.print_error(error_details)
        print(traceback.format_exc())
        generate_and_log_summary({}, pd.DataFrame(), "", success=False, error_details=error_details)
        sys.exit(1) 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consolidate order data.")
    parser.add_argument("staging_dir", help="Full path to the single staging directory.")
    parser.add_argument("file_paths_map_json", help="JSON string mapping keys to full file paths.")
    args = parser.parse_args()

    try:
        file_paths_map_dict = json.loads(args.file_paths_map_json)
    except Exception as e:
        utils_ui.print_error(f"Invalid JSON args: {e}")
        sys.exit(1)

    main(staging_dir=args.staging_dir, file_paths_map=file_paths_map_dict)
