import pandas as pd
import os

def filter_mapping_data():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.abspath(os.path.join(BASE_DIR, '../../cleaned_data/cik_ticker_permno_mapping.csv'))
    output_dir = os.path.abspath(os.path.join(BASE_DIR, '../../raw_data'))
    output_path = os.path.join(output_dir, 'cik_ticker_permno_mapping.csv')
    
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        print(f"Reading file: {input_path}")
        df = pd.read_csv(input_path)
        print(f"Original row count: {df.shape[0]}")
        
        df['linkenddt_parsed'] = pd.to_datetime(df['linkenddt'], errors='coerce')
        
        # Filter for links active after 2012-01-01
        df_filtered = df[df['linkenddt_parsed'] >= pd.to_datetime('2012-01-01')].copy()
        df_filtered = df_filtered.drop(columns=['linkenddt_parsed'])
        
        df_filtered.to_csv(output_path, index=False)
        print(f"Filtered row count: {df_filtered.shape[0]}")
        print(f"✅ Success! File saved to: {output_path}")
        
    except FileNotFoundError:
        print(f"❌ Error: File not found {input_path}")
    except Exception as e:
        print(f"❌ Error processing file: {e}")

if __name__ == "__main__":
    filter_mapping_data()
