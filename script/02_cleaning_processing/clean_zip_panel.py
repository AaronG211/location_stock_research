import pandas as pd
import numpy as np
import os

def clean_zip_panel(input_path, output_path):
    print(f"Loading panel for cleaning: {input_path}")
    # Read original wide table
    df = pd.read_csv(input_path, dtype=str, index_col=0)
    
    print("Applying cleaning rules (Vectorized acceleration)...")
    original_cols = df.shape[1]
    
    # Technique: Flatten wide table to 1D Series for high-speed text cleaning
    s = df.stack(future_stack=True)
    
    # Rule 1: Remove all leading/trailing whitespace
    s = s.str.strip()
    
    # Rule 2: Keep only the first 5 characters
    s = s.str[:5]
    
    # Rule 3: Replace ZIPs consisting only of '0' with NaN
    # Regex r'^0+$' matches strings containing only '0'
    s = s.replace(r'^0+$', pd.NA, regex=True)
    
    # Rule 4: Remove ZIPs containing letters
    # Regex [A-Za-z] captures all uppercase and lowercase letters
    has_letters_mask = s.str.contains(r'[A-Za-z]', regex=True, na=False)
    s.loc[has_letters_mask] = pd.NA
    
    # Treat empty strings or unrecognized whitespace as NA
    s = s.replace('', pd.NA)
    
    print("Reconstructing cleaned 2D panel...")
    # Convert back to original 2D wide table structure
    df_cleaned = s.unstack()
    
    # Rule 5: Drop columns that contain no valid ZIP codes
    # dropna(axis=1, how='all') drops columns where all values are NaN
    df_cleaned = df_cleaned.dropna(axis=1, how='all')
    
    surviving_cols = df_cleaned.shape[1]
    dropped_cols = original_cols - surviving_cols
    
    print(f"Purged {dropped_cols} permnos with invalid/empty data history.")
    print(f"Final clean matrix: {df_cleaned.shape[0]} months x {surviving_cols} valid permnos.")
    
    # Keep output directory clean
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_cleaned.to_csv(output_path)
    print(f"Cleaned data saved to: {output_path}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    INPUT_FILE = os.path.abspath(os.path.join(BASE_DIR, '../../cleaned_data/monthly_zip_panel_180mo.csv'))
    OUTPUT_FILE = os.path.abspath(os.path.join(BASE_DIR, '../../cleaned_data/monthly_zip_panel_180mo_cleaned.csv'))
    
    clean_zip_panel(INPUT_FILE, OUTPUT_FILE)
