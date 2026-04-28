import pandas as pd
import os

def filter_lm_header(input_csv, output_csv):
    """
    Filter Loughran-McDonald 10-K Header dataset to retain strictly geographic and ID columns.
    """
    columns_mapping = {
        'filing_firm_cik': 'CIK',
        'filing_date': 'FILING_DATE',
        'ba_state': 'BUS_STATE',
        'ba_city': 'BUS_CITY',
        'ba_zip': 'BUS_ZIP',
        'ma_state': 'MAIL_STATE',
        'ma_city': 'MAIL_CITY',
        'ma_zip': 'MAIL_ZIP'
    }
    
    print(f"Reading file: {input_csv} ...")
    
    try:
        use_cols = list(columns_mapping.keys())
        dtypes_dict = {'filing_firm_cik': str, 'ba_zip': str, 'ma_zip': str}
        
        df = pd.read_csv(input_csv, usecols=use_cols, dtype=dtypes_dict, low_memory=False)
        df = df.rename(columns=columns_mapping)
        
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        df.to_csv(output_csv, index=False)
        
        print(f"Filtered complete! Kept {df.shape[0]} rows, {df.shape[1]} columns.")
        print(f"Output saved to: {output_csv}")

    except FileNotFoundError:
        print(f"Error: File '{input_csv}' not found.")
    except Exception as e:
        print(f"Unknown error: {e}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    INPUT_FILE = os.path.abspath(os.path.join(BASE_DIR, '../../raw_data/LoughranMcDonald_10-K_HeaderData_1993-2024.csv'))
    OUTPUT_FILE = os.path.abspath(os.path.join(BASE_DIR, '../../cleaned_data/LM_10K_Headers_Geo_Filtered.csv'))
    
    filter_lm_header(INPUT_FILE, OUTPUT_FILE)
