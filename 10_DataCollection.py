# 10_DataCollection.py
# --- Dependencies and Setup ---
import pandas as pd
import numpy as np
import datetime
import os
import shutil
import traceback
import glob # Keep glob for potential future use, though not needed now
# --- Logging/Sys ---
import sys
import json               
import argparse           
from typing import List, Dict, Tuple, Any
from datetime import datetime, timedelta, date # <-- ADDED

# --- Holiday Libraries ---
try:
    import holidays
    from holidays.countries import UnitedStates
except ImportError:
    print("FATAL ERROR: Required library not found. Please install 'holidays' by running:")
    print("pip install holidays")
    sys.exit(1)


# --- CONFIGURATION ---

# --- Log Init Removed ---

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

# --- Logger Setup Removed ---

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes all column names in a DataFrame.
    """
    new_cols = []
    for col in df.columns:
        new_col = str(col).lower().strip().replace(' ', '_').replace('#', 'num')
        new_cols.append(new_col)
    df.columns = new_cols
    return df

def load_data(file_paths_map: Dict[str, str]) -> Tuple[Dict[str, pd.DataFrame], Dict[str, str]]:
    """
    Loads source XLSX files from the exact paths provided.
    """
    dataframes = {}
    print("Loading source files from provided paths...") # Use print

    for key, path in file_paths_map.items():
        print(f"Attempting to load '{key}' from: {path}") # Use print
        if not os.path.exists(path):
            raise FileNotFoundError(f"Error: Provided file for '{key}' not found at: {path}")

        filename = os.path.basename(path)

        # Read from Excel
        try:
            df = pd.read_excel(path, engine='openpyxl')
        except Exception as e:
            print(f"Failed to read Excel file {filename}: {e}") # Use print
            raise # Re-raise the exception to stop the process

        df = clean_column_names(df)
        dataframes[key] = df
        print(f"Successfully loaded and cleaned: {filename}") # Use print

    return dataframes, file_paths_map


def preprocess_data(dataframes: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Ensures key columns have consistent data types for merging.
    """
    for df in dataframes.values():
        if 'job_ticket_number' in df.columns:
            df['job_ticket_number'] = df['job_ticket_number'].astype(str)
        if 'sku' in df.columns:
            df['sku'] = df['sku'].astype(str)
        if 'order_item_id' in df.columns:
            # CRITICAL: Ensure this remains 'Int64' to allow for pd.NA (missing values)
            df['order_item_id'] = pd.to_numeric(df['order_item_id'], errors='coerce').astype('Int64')
    print("\nPre-processing complete: Ensured key columns have consistent data types.") # Use print
    return dataframes

def merge_data(dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Executes the sequential merge strategy.
    """
    df_primary_1 = dataframes['job_ticket'].copy()
    df_secondary_1 = dataframes['order_ship'].copy()
    df_primary_1['tie_breaker'] = df_primary_1.groupby(['job_ticket_number', 'sku']).cumcount()
    df_secondary_1['tie_breaker'] = df_secondary_1.groupby(['job_ticket_number', 'sku']).cumcount()
    merged_df_1 = pd.merge(df_primary_1, df_secondary_1, on=['job_ticket_number', 'sku', 'tie_breaker'], how='outer', suffixes=('', f"_from_order_ship"))
    print("Step 1/2 Complete: Merged 'job_ticket' data with 'order_ship' data.") # Use print

    df_secondary_2 = dataframes['total_order'].copy()
    final_df = pd.merge(merged_df_1, df_secondary_2, on='order_item_id', how='outer', suffixes=('', f"_from_total_order"))
    print("Step 2/2 Complete: Merged financial data from 'total_order' data.") # Use print

    if 'tie_breaker' in final_df.columns:
        final_df = final_df.drop(columns=['tie_breaker'])
    return final_df

def finalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorders columns (using global config) and sorts the final DataFrame.
    """
    print("\nFinalizing report structure.") # Use print
    existing_cols = [col for col in PREFERRED_COLUMN_ORDER if col in df.columns]
    other_cols = sorted([col for col in df.columns if col not in existing_cols])
    df = df[existing_cols + other_cols]
    if 'job_ticket_number' in df.columns and 'sku' in df.columns:
        df = df.sort_values(by=['job_ticket_number', 'sku'], ascending=True)
    return df

# --- Function to generate the box_X columns ---
def generate_box_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the eight new box_X columns by concatenating the numeric 
    order_item_id (as a string) with a single uppercase letter.
    """
    print("\nGenerating box_A through box_H columns...")
    
    if 'order_item_id' not in df.columns:
        print("WARNING: 'order_item_id' column not found. Skipping box column generation.")
        return df
        
    # Convert 'order_item_id' to string, replacing <NA> with an empty string.
    # The .astype(str) conversion from Int64 results in the literal string "<NA>" for nulls.
    order_item_str = df['order_item_id'].astype(str).str.replace('<NA>', '').str.replace('nan', '')
    
    box_cols_and_suffixes = {
        'box_A': 'A', 'box_B': 'B', 'box_C': 'C', 'box_D': 'D',
        'box_E': 'E', 'box_F': 'F', 'box_G': 'G', 'box_H': 'H'
    }
    
    for col_name, suffix in box_cols_and_suffixes.items():
        # Concatenate the string value (which is numeric or empty) with the suffix
        df[col_name] = order_item_str + suffix
        
    print("✓ Box column generation complete.")
    return df
# --- Generator Ends ---

def clean_dataframe_for_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the final DataFrame for output.
    """
    print("\nCleaning 'object' columns for final output (replacing NaN with '')...") # Use print
    df_clean = df.copy()
    object_cols = df_clean.select_dtypes(include=['object']).columns
    df_clean[object_cols] = df_clean[object_cols].fillna('')
    print("Cleaning complete.") # Use print
    return df_clean



def generate_and_log_summary(
    source_files: Dict[str, str],
    final_report_df: pd.DataFrame,
    # new_blank_rows argument REMOVED
    file_path: str,
    success: bool = True,
    error_details: str = ""
) -> None:
    """
    Generates summary content and prints it.
    """
    if success:
        summary_lines = [
            "\n", "==================================================",
            f"--- Consolidation Summary: {datetime.now().strftime('%Y-%m-%d')} ---",
            "Process completed successfully.\n", "- Source Files Used:",
        ]
        summary_lines.extend([f"  - {os.path.basename(path)}" for path in source_files.values()])
        summary_lines.extend([
            "\n- Final Report:", f"  - Path: {file_path}",
            f"  - Total Rows: {len(final_report_df)}",
            f"  - Total Columns: {len(final_report_df.columns)}\n",
            # Removed Data Quality Log section regarding blank product_ids
            "==================================================\n"
        ])
        print("\n".join(summary_lines)) # Use print
    else:
        summary_lines = [
            "\n", "==================================================",
            f"--- Consolidation Summary: {datetime.now().strftime('%Y-%m-%d')} ---",
            "PROCESS FAILED.\n", f"- Error Details: {error_details}",
            "==================================================\n"
        ]
        print("\n".join(summary_lines)) # Use print

# --- Main Function ---
def main(staging_dir: str, file_paths_map: Dict[str, str]) -> None:
    """
    Main function to execute the entire data consolidation pipeline.

    Args:
        staging_dir: The full path to the single '_STAGING' directory.
        file_paths_map: A dictionary mapping keys to their exact, full file paths.
    """
        # --- Logging Setup Removed ---
    print("--- Starting 10_DataCollection Script ---") # Use print

    source_files, final_report_df = {}, pd.DataFrame()
    # Removed new_blank_rows initialization

    try:
        # --- Create staging directory ---
        os.makedirs(staging_dir, exist_ok=True)

        # 1. Load data from exact paths
        all_data, source_files = load_data(file_paths_map)

        # 2. Process data
        all_data = preprocess_data(all_data)
        merged_df = merge_data(all_data)
        final_report_df = finalize_dataframe(merged_df)
        
        # --- Generate the box columns ---
        final_report_df = generate_box_columns(final_report_df)

        # 3. Dynamic file naming
        output_file_name = f'Consolidated_Report_{datetime.now().strftime("%Y-%m-%d")}.xlsx'
        
        # --- Ship Date calculation ---
        if 'order_date' in final_report_df.columns:
            # First, convert the 'order_date' column
            final_report_df['order_date'] = pd.to_datetime(final_report_df['order_date'], errors='coerce')
            
            # --- Calculate Ship Date ---
            print("\nCalculating business-aware ship dates...")
            final_report_df['ship_date'] = final_report_df['order_date'].apply(calculate_ship_date)
            print("✓ Ship date calculation complete.")
            
            # Now, use the converted dates for the filename logic
            order_dates = final_report_df['order_date'].dt.date.dropna() # Get dates from the column
            if not order_dates.empty:
                min_date, max_date = order_dates.min(), order_dates.max()
                min_date_str, max_date_str = min_date.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d')
                output_file_name = f'MarcomOrderDate {min_date_str}.xlsx' if min_date == max_date else f'MarcomOrderDate {min_date_str}_to_{max_date_str}.xlsx'


        # 4. Construct output paths
        # --- Report is now saved to staging_dir ---
        output_file_path = os.path.join(staging_dir, output_file_name)

        # 5. Workflow: Clean, Save (Removed Log update)
        # update_product_id_log call REMOVED
        cleaned_report_df = clean_dataframe_for_output(final_report_df)
        cleaned_report_df.to_excel(output_file_path, index=False, engine='openpyxl')
        print(f"\nSuccessfully created the consolidated report: {output_file_path}") # Use print

        # 6. Move source files
        # --- Move Source Files ---
        print("\n--- Moving Source Files ---") # Use print
        for source_key, source_path in source_files.items(): # Iterate through the map
             if os.path.exists(source_path):
                 filename = os.path.basename(source_path)
                 dest_path = os.path.join(staging_dir, filename) # <-- Move to staging
                 try:
                     shutil.move(source_path, dest_path)
                     print(f"Moved '{filename}' to '{staging_dir}' folder.") # Use print
                 except Exception as move_err:
                     print(f"Failed to move '{filename}': {move_err}") # Use print
             else:
                 print(f"Source file for '{source_key}' not found at '{source_path}' during move operation.") # Use print


        # Updated call to generate_and_log_summary (Removed new_blank_rows)
        generate_and_log_summary(source_files, final_report_df, output_file_path, success=True)
        print("\n--- Process Complete ---") # Use print

    except (FileNotFoundError, FileExistsError) as e:
        error_details = str(e)
        print(f"\nERROR: A file-related problem occurred. {error_details}") # Use print
        print(f"Traceback:\n{traceback.format_exc()}") # Use print
        # Updated call to generate_and_log_summary (Removed numeric argument)
        generate_and_log_summary({}, pd.DataFrame(), "", success=False, error_details=error_details)
        sys.exit(1) # Exit with error code for controller
    except Exception as e:
        error_details = f"An unexpected error occurred: {e}"
        print(f"\n{error_details}") # Use print
        print(f"Traceback:\n{traceback.format_exc()}") # Use print
        # Updated call to generate_and_log_summary (Removed numeric argument)
        generate_and_log_summary({}, pd.DataFrame(), "", success=False, error_details=error_details)
        sys.exit(1) # Exit with error code for controller


# --- Argparse Block ---
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consolidate order data from three Excel files.")
    # --- Replaced output_dir with staging_dir ---
    parser.add_argument("staging_dir", help="Full path to the single staging directory for all outputs.")
    parser.add_argument("file_paths_map_json", help="JSON string mapping keys ('job_ticket', 'total_order', 'order_ship') to full file paths.")

    args = parser.parse_args()

    # Parse the JSON string back into a dictionary
    try:
        file_paths_map_dict = json.loads(args.file_paths_map_json)
        if not isinstance(file_paths_map_dict, dict) or len(file_paths_map_dict) != 3:
             raise ValueError("file_paths_map_json must be a JSON object with 3 keys.")
        if not all(k in file_paths_map_dict for k in ['job_ticket', 'total_order', 'order_ship']):
             raise ValueError("JSON map must be a JSON object with 3 keys.")
    except json.JSONDecodeError:
        print("FATAL ERROR: Invalid JSON string provided for file_paths_map_json.")
        sys.exit(1)
    except ValueError as ve:
        print(f"FATAL ERROR: {ve}")
        sys.exit(1)

    # Call the main function with parsed arguments
    main(
        staging_dir=args.staging_dir,
        file_paths_map=file_paths_map_dict
    )
