import pandas as pd
import os

TARGET_START_MONTH = '2008-01'
TARGET_END_MONTH = '2022-12'


def build_zip_panel(input_path, output_path):
    print(f"Reading dataset: {input_path}")
    df = pd.read_csv(input_path, dtype=str)
    
    print("Determining ZIP codes (Priority: Business ZIP > Mail ZIP)...")
    empty_vals = ['nan', 'NaN', 'None', '', '<NA>', ' ']
    df['BUS_ZIP'] = df['BUS_ZIP'].replace(empty_vals, pd.NA)
    df['MAIL_ZIP'] = df['MAIL_ZIP'].replace(empty_vals, pd.NA)
    
    df['TARGET_ZIP'] = df['BUS_ZIP'].fillna(df['MAIL_ZIP'])
    df = df.dropna(subset=['permno', 'TARGET_ZIP'])
    
    print("Converting timestamps and slicing into monthly periods...")
    df['DATE'] = pd.to_datetime(df['FILING_DATE'].astype(str), format='%Y%m%d', errors='coerce')
    df['DATE'] = df['DATE'].fillna(pd.to_datetime(df['FILING_DATE'].astype(str), errors='coerce'))
    df = df.dropna(subset=['DATE'])
    
    df['MONTH_PERIOD'] = df['DATE'].dt.to_period('M')
    
    # Handle multiple filings within a single month per permno
    df = df.sort_values(by='DATE')
    df = df.drop_duplicates(subset=['permno', 'MONTH_PERIOD'], keep='last')
    
    print("Generating wide-format panel (Months x Permnos)...")
    pivot_df = df.pivot(index='MONTH_PERIOD', columns='permno', values='TARGET_ZIP')
    
    print("Applying Forward-Fill for data continuity (Last available ZIP)...")
    min_month = pivot_df.index.min()
    max_month = pd.Period(TARGET_END_MONTH, freq='M')
    full_range = pd.period_range(start=min_month, end=max_month, freq='M')
    
    pivot_df = pivot_df.reindex(full_range)
    pivot_df = pivot_df.ffill()
    
    print(
        f"Slicing core research window ({TARGET_START_MONTH} to "
        f"{TARGET_END_MONTH} | 180 Months)..."
    )
    target_months = pd.period_range(start=TARGET_START_MONTH, end=TARGET_END_MONTH, freq='M')
    final_panel = pivot_df.reindex(target_months)
    
    final_panel.index = final_panel.index.astype(str)
    final_panel.index.name = 'Month'
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_panel.to_csv(output_path)
    
    print(f"Panel generated successfully!")
    print(f"Matrix dimensions: {final_panel.shape[0]} months x {final_panel.shape[1]} permnos")
    print(f"Output saved to: {output_path}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    INPUT_FILE = os.path.abspath(os.path.join(BASE_DIR, '../../cleaned_data/merged_geo_ticker_data.csv'))
    OUTPUT_FILE = os.path.abspath(os.path.join(BASE_DIR, '../../cleaned_data/monthly_zip_panel_180mo.csv'))
    
    build_zip_panel(INPUT_FILE, OUTPUT_FILE)
