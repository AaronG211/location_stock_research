import pandas as pd
import os

def merge_geo_and_stock(geo_path, mapping_path, output_path):
    print("Loading geographic and stock mapping datasets...")
    
    df_geo = pd.read_csv(geo_path, dtype={'CIK': str, 'BUS_ZIP': str, 'MAIL_ZIP': str})
    df_map = pd.read_csv(mapping_path, dtype={'cik': str})
    
    # -------------------------------
    # Step 1. Normalization
    # -------------------------------
    print("Normalizing CIK and Date formats...")
    df_geo['CIK'] = df_geo['CIK'].astype(str).str.zfill(10)
    df_map['cik'] = df_map['cik'].astype(str).str.zfill(10)
    
    df_geo['FILING_DATE_DT'] = pd.to_datetime(df_geo['FILING_DATE'], format='%Y%m%d', errors='coerce')
    df_map['linkdt'] = pd.to_datetime(df_map['linkdt'], errors='coerce')
    df_map['linkenddt'] = pd.to_datetime(df_map['linkenddt'], errors='coerce')
    
    # -------------------------------
    # Step 2. Execute Merge
    # -------------------------------
    print("Executing Time-Join (Filing date must be within ticker link duration)...")
    merged_df = pd.merge(df_geo, df_map, left_on='CIK', right_on='cik', how='inner')
    
    valid_mask = (merged_df['FILING_DATE_DT'] >= merged_df['linkdt']) & \
                 (merged_df['FILING_DATE_DT'] <= merged_df['linkenddt'])
                 
    final_df = merged_df[valid_mask].copy()
    final_df = final_df.drop(columns=['FILING_DATE_DT', 'cik'])
    
    # -------------------------------
    # Step 3. Output Results
    # -------------------------------
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_df.to_csv(output_path, index=False)
    
    print(f"✅ Merge complete! Mapped {len(final_df)} filing records.")
    print(f"✅ File saved to: {output_path}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    GEO_FILE = os.path.abspath(os.path.join(BASE_DIR, '../../cleaned_data/LM_10K_Headers_Geo_Filtered.csv'))
    MAPPING_FILE = os.path.abspath(os.path.join(BASE_DIR, '../../raw_data/cik_ticker_permno_mapping.csv'))
    OUTPUT_FILE = os.path.abspath(os.path.join(BASE_DIR, '../../cleaned_data/merged_geo_ticker_data.csv'))
    
    merge_geo_and_stock(GEO_FILE, MAPPING_FILE, OUTPUT_FILE)
