import os
import pandas as pd
import glob
from pathlib import Path

def parse_ff48(filepath):
    """
    智能解析 Fama-French 48 Industry 文本文件，
    将其转化为字典： { 101: 'Agriculture', 102: 'Agriculture', ... }。
    """
    sic_to_ind = {}
    current_industry = "Unknown"
    
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            # 判断是不是大类的标题行，特征是数字开头且没有连字符 "-" (比如 " 1 Agric  Agriculture")
            if line[0].isdigit() and '-' not in line[:10]:
                parts = line.split(maxsplit=2)
                if len(parts) >= 3:
                    current_industry = parts[2].strip() # 提取全称
                continue
                
            # 判断是不是区间代码行，特征是包含连字符 (比如 "0100-0199 Agricultural production")
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
    """处理 SIC，从字典中获取 Industry 分类"""
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
    
    for i, file in enumerate(csv_files):
        df = pd.read_csv(file, dtype={'sic': str, 'SIC': str})
        
        if df.empty:
            continue
            
        # 1. Force conversion of prc and ret to numeric (coerce errors like 'C' to NaN)
        if 'prc' in df.columns:
            df['prc'] = pd.to_numeric(df['prc'], errors='coerce')
        if 'ret' in df.columns:
            df['ret'] = pd.to_numeric(df['ret'], errors='coerce')
            
        # 2. Drop rows missing essential price/return data
        required_cols = [c for c in ['prc', 'ret'] if c in df.columns]
        if required_cols:
            df = df.dropna(subset=required_cols)
            
        # Discard files that become empty after filtering (e.g., short-lived or low-quality stocks)
        if df.empty:
            deleted_files_count += 1
            continue
            
        # 3. Standardize market_cap to dollars (multiply by 1000)
        if 'market_cap' in df.columns:
            df['market_cap'] = df['market_cap'] * 1000
            
        # 4. Map Industry based on SIC codes
        sic_col = 'sic' if 'sic' in df.columns else ('SIC' if 'SIC' in df.columns else None)
        if sic_col:
            df['industry'] = df[sic_col].apply(lambda x: map_industry(x, sic_dict))
            
        # Save cleaned data to destination
        basename = os.path.basename(file)
        df.to_csv(os.path.join(clean_dir, basename), index=False)
        
        if (i+1) % 1500 == 0:
            print(f"Progress: {i+1} / {total_files} ...")

    print(f"🎉 Cleaning complete! Total valid output files: {total_files - deleted_files_count}")
    print(f"Total stocks removed due to missing data: {deleted_files_count}")
    print(f"✅ Cleaned files saved in: {clean_dir}")

if __name__ == "__main__":
    clean_stock_prices()
