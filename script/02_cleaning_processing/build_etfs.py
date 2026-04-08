import os
import pandas as pd
import glob
import numpy as np

def safe_wavg(group, data_col, weight_col):
    """安全地计算市值加权平均数，避免全市场市值为空或加了 0 进而报错"""
    d = group[data_col]
    w = group[weight_col]
    if w.sum() == 0 or pd.isna(w.sum()):
        return np.nan
    return (d * w).sum() / w.sum()

def build_etfs():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    stock_dir = os.path.join(BASE_DIR, '../../cleaned_data/stock_price')
    msa_panel_path = os.path.join(BASE_DIR, '../../cleaned_data/monthly_msa_panel_144mo.csv')
    
    out_dir = os.path.join(BASE_DIR, '../../cleaned_data/etf_price')
    os.makedirs(out_dir, exist_ok=True)
    
    # ------------------
    # Step 1: Melt MSA wide panel to long format for easy merging
    # ------------------
    print("Melting MSA geographic panel into long format...")
    msa_df = pd.read_csv(msa_panel_path, index_col=0)
    msa_df.index.name = 'Month'
    msa_long = msa_df.reset_index().melt(id_vars=['Month'], var_name='Ticker', value_name='Location_MSA')
    msa_long.dropna(subset=['Location_MSA'], inplace=True)
    
    # Handle Location_MSA as string to avoid precision issues
    msa_long['Location_MSA'] = msa_long['Location_MSA'].apply(lambda x: str(int(x)) if pd.notna(x) and str(x).replace('.','').isdigit() else x)

    # ------------------
    # Step 2: Aggregate individual stock files into a master panel
    # ------------------
    print("Assembling master panel from individual stock files (may take a few seconds)...")
    csv_files = glob.glob(os.path.join(stock_dir, "*.csv"))
    
    all_stocks = []
    
    for i, file in enumerate(csv_files):
        df = pd.read_csv(file)
        if df.empty or 'ret' not in df.columns or 'market_cap' not in df.columns:
            continue
            
        filename = os.path.basename(file)
        ticker = filename.split('_')[0]
        
        # Standardize date to Month (YYYY-MM)
        df['Month'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m')
        df['Ticker'] = ticker
        
        if 'industry' not in df.columns:
            df['industry'] = pd.NA
            
        df_subset = df[['Month', 'Ticker', 'ret', 'market_cap', 'industry']].copy()
        
        df_subset['ret'] = pd.to_numeric(df_subset['ret'], errors='coerce')
        df_subset['market_cap'] = pd.to_numeric(df_subset['market_cap'], errors='coerce')
        df_subset.dropna(subset=['ret', 'market_cap'], inplace=True)
        
        all_stocks.append(df_subset)
        
    master_df = pd.concat(all_stocks, ignore_index=True)
    
    # ------------------
    # Step 3: Merge Geographic Location (MSA) data
    # ------------------
    print("Merging geographic MSA metadata into the panel...")
    merged_df = pd.merge(master_df, msa_long, on=['Month', 'Ticker'], how='left')
    
    # ------------------
    # Step 4: Compute the 5 core Benchmark ETFs (Simple and Value-Weighted)
    # ------------------
    print("Calculating returns for 5 core ETF strategies (Equal and Market-Cap Weighted)...")
    
    # 1. location_simple (Equal-weighted by city)
    loc_simple = merged_df.dropna(subset=['Location_MSA']).groupby(['Month', 'Location_MSA'])['ret'].mean().unstack()
    loc_simple.to_csv(os.path.join(out_dir, 'location_simple.csv'))
    
    # 2. location_weighted (Value-weighted by city)
    loc_weighted = merged_df.dropna(subset=['Location_MSA']).groupby(['Month', 'Location_MSA']).apply(
        lambda g: safe_wavg(g, 'ret', 'market_cap')
    ).unstack()
    loc_weighted.to_csv(os.path.join(out_dir, 'location_weighted.csv'))
    
    # 3. industry_simple (Equal-weighted by industry)
    ind_simple = merged_df.dropna(subset=['industry']).groupby(['Month', 'industry'])['ret'].mean().unstack()
    ind_simple.to_csv(os.path.join(out_dir, 'industry_simple.csv'))
    
    # 4. industry_weighted (Value-weighted by industry)
    ind_weighted = merged_df.dropna(subset=['industry']).groupby(['Month', 'industry']).apply(
        lambda g: safe_wavg(g, 'ret', 'market_cap')
    ).unstack()
    ind_weighted.to_csv(os.path.join(out_dir, 'industry_weighted.csv'))
    
    # 5. market_weighted (Total market value-weighted benchmark)
    mkt_weighted = merged_df.groupby('Month').apply(
        lambda g: safe_wavg(g, 'ret', 'market_cap')
    ).to_frame(name='Market_Weighted_Return')
    mkt_weighted.to_csv(os.path.join(out_dir, 'market_weighted.csv'))
    
    print(f"🎉 Generation of 5 ETFs concluded successfully! Files available in: {out_dir}")

if __name__ == "__main__":
    build_etfs()
