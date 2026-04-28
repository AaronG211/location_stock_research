import os
import pandas as pd
import glob
from pathlib import Path

COMMON_SHARE_CODES = {'10', '11', '10.0', '11.0'}
PRIMARY_EXCHANGE_CODES = {'1', '2', '3', '1.0', '2.0', '3.0'}


def parse_ff48(filepath):
    """Parse Fama-French 48 industry text file into a dict: { 101: 'Agriculture', ... }."""
    sic_to_ind = {}
    current_industry = "Unknown"

    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Industry header line: starts with a digit and has no hyphen in first 10 chars
            # e.g. " 1 Agric  Agriculture"
            if line[0].isdigit() and '-' not in line[:10]:
                parts = line.split(maxsplit=2)
                if len(parts) >= 3:
                    current_industry = parts[2].strip()
                continue

            # SIC range line: contains a hyphen e.g. "0100-0199 Agricultural production"
            if '-' in line[:10]:
                range_part = line.split()[0]
                try:
                    start_str, end_str = range_part.split('-')
                    start, end = int(start_str), int(end_str)
                    for sic in range(start, end + 1):
                        sic_to_ind[sic] = current_industry
                except ValueError:
                    continue
                    
    return sic_to_ind

def map_industry(sic, sic_dict):
    """Map a SIC code to its FF48 industry label."""
    if pd.isna(sic):
        return pd.NA
    try:
        sic_int = int(float(sic))
        return sic_dict.get(sic_int, "Other")
    except:
        return "Other"

def clean_stock_prices():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    raw_dir = os.path.join(BASE_DIR, '../../raw_data/stock_price')
    clean_dir = os.path.join(BASE_DIR, '../../cleaned_data/stock_price')
    ff_path = os.path.join(BASE_DIR, '../../raw_data/Fama_French_Industries.txt')
    
    os.makedirs(clean_dir, exist_ok=True)
    
    print("Parsing Fama-French industry structure...")
    sic_dict = parse_ff48(ff_path)
    
    csv_files = glob.glob(os.path.join(raw_dir, "*.csv"))
    total_files = len(csv_files)
    print(f"Scanned {total_files} stock data files for processing.")
    
    deleted_files_count = 0
    dropped_non_common_files = 0
    
    for i, file in enumerate(csv_files):
        df = pd.read_csv(file, dtype={'sic': str, 'SIC': str, 'shrcd': str, 'SHRCD': str})
        
        if df.empty:
            continue

        # 0. Keep only the standard CRSP universe used in replication work:
        # common shares on NYSE / AMEX / NASDAQ. The raw pull stays broad, and
        # all sample screens happen here.
        shrcd_col = 'shrcd' if 'shrcd' in df.columns else ('SHRCD' if 'SHRCD' in df.columns else None)
        if shrcd_col:
            df = df[df[shrcd_col].astype(str).str.strip().isin(COMMON_SHARE_CODES)]

        exchcd_col = 'exchcd' if 'exchcd' in df.columns else ('EXCHCD' if 'EXCHCD' in df.columns else None)
        if exchcd_col:
            df = df[df[exchcd_col].astype(str).str.strip().isin(PRIMARY_EXCHANGE_CODES)]
            
        if df.empty:
            deleted_files_count += 1
            dropped_non_common_files += 1
            continue

        # 1. Exclude REITs and Closed-End Funds based on SIC codes
        sic_col = 'sic' if 'sic' in df.columns else ('SIC' if 'SIC' in df.columns else None)
        if sic_col:
            # 6798: REITs, 6722: Management Investment Offices, 6726: Unit Investment Trusts (Closed-End)
            invalid_sics = ['6798', '6722', '6726', '6798.0', '6722.0', '6726.0']
            df = df[~df[sic_col].astype(str).str.strip().isin(invalid_sics)]
            
        if df.empty:
            deleted_files_count += 1
            continue
            
        # 2. Force conversion of prc and ret to numeric (coerce errors like 'C' to NaN)
        if 'prc' in df.columns:
            df['prc'] = pd.to_numeric(df['prc'], errors='coerce')
        if 'ret' in df.columns:
            df['ret'] = pd.to_numeric(df['ret'], errors='coerce')
            
        # 3. Drop rows missing essential price/return data
        required_cols = [c for c in ['prc', 'ret'] if c in df.columns]
        if required_cols:
            df = df.dropna(subset=required_cols)
            
        # Discard files that become empty after filtering (e.g., short-lived or low-quality stocks)
        if df.empty:
            deleted_files_count += 1
            continue
            
        # 4. Standardize market_cap to dollars (multiply by 1000)
        if 'market_cap' in df.columns:
            df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce') * 1000
            df = df[df['market_cap'] > 0]
            
        # 5. Map Industry based on SIC codes
        sic_col = 'sic' if 'sic' in df.columns else ('SIC' if 'SIC' in df.columns else None)
        if sic_col:
            df['industry'] = df[sic_col].apply(lambda x: map_industry(x, sic_dict))
            
        # Save cleaned data to destination
        basename = os.path.basename(file)
        df.to_csv(os.path.join(clean_dir, basename), index=False)
        
        if (i+1) % 1500 == 0:
            print(f"Progress: {i+1} / {total_files} ...")

    print(f"Cleaning complete! Total valid output files: {total_files - deleted_files_count}")
    print(f"Total stocks removed due to missing data: {deleted_files_count}")
    print(f"  of which removed at common-share screen: {dropped_non_common_files}")
    print(f"Cleaned files saved in: {clean_dir}")

if __name__ == "__main__":
    clean_stock_prices()
