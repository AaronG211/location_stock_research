import os
import pandas as pd
import glob
import numpy as np

def safe_wavg(group, data_col, weight_col):
    """Compute value-weighted average, returning NaN if total weight is zero or missing."""
    d = group[data_col]
    w = group[weight_col]
    if w.sum() == 0 or pd.isna(w.sum()):
        return np.nan
    return (d * w).sum() / w.sum()

def build_etfs():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    stock_dir = os.path.join(BASE_DIR, '../../cleaned_data/stock_price')
    msa_panel_path = os.path.join(BASE_DIR, '../../cleaned_data/monthly_msa_panel_180mo.csv')
    
    out_dir = os.path.join(BASE_DIR, '../../cleaned_data/etf_price')
    os.makedirs(out_dir, exist_ok=True)
    
    # ------------------
    # Step 1: Melt MSA wide panel to long format for easy merging
    # ------------------
    print("Melting MSA geographic panel into long format...")
    msa_df = pd.read_csv(msa_panel_path, index_col=0, dtype=str)
    msa_df.index.name = 'Month'
    msa_df.columns = [str(col).replace('.0', '') for col in msa_df.columns]
    msa_long = msa_df.reset_index().melt(id_vars=['Month'], var_name='permno', value_name='Location_MSA')
    msa_long['permno'] = msa_long['permno'].astype(str).str.replace('.0', '', regex=False)
    msa_long.dropna(subset=['Location_MSA'], inplace=True)
    
    # Handle Location_MSA as string to avoid precision issues
    msa_long['Location_MSA'] = msa_long['Location_MSA'].apply(lambda x: str(int(x)) if pd.notna(x) and str(x).replace('.','').isdigit() else x)

    # ------------------
    # Step 2: Aggregate individual stock files into a master panel
    # ------------------
    print("Assembling master panel from individual stock files (may take a few seconds)...")
    csv_files = glob.glob(os.path.join(stock_dir, "*.csv"))
    eligible_permnos = set(msa_df.columns)
    
    all_stocks = []
    
    for i, file in enumerate(csv_files):
        df = pd.read_csv(file)
        if df.empty or 'ret' not in df.columns or 'market_cap' not in df.columns:
            continue
            
        filename = os.path.basename(file)
        permno = filename.split('_')[1].split('.')[0]
        if permno not in eligible_permnos:
            continue
        
        # Standardize date to Month (YYYY-MM)
        df['Month'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m')
        df['permno'] = permno
        
        if 'industry' not in df.columns:
            df['industry'] = pd.NA
            
        df_subset = df[['Month', 'permno', 'ret', 'market_cap', 'industry']].copy()
        
        df_subset['ret'] = pd.to_numeric(df_subset['ret'], errors='coerce')
        df_subset['market_cap'] = pd.to_numeric(df_subset['market_cap'], errors='coerce')
        df_subset.dropna(subset=['ret', 'market_cap'], inplace=True)
        
        all_stocks.append(df_subset)
        
    master_df = pd.concat(all_stocks, ignore_index=True)
    
    # ------------------
    # Step 3: Merge Geographic Location (MSA) data
    # ------------------
    print("Merging geographic MSA metadata into the panel...")
    merged_df = pd.merge(master_df, msa_long, on=['Month', 'permno'], how='left')
    
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
    
    # 5. Sample-derived market benchmark from the eligible stock universe
    mkt_weighted = merged_df.groupby('Month').apply(
        lambda g: safe_wavg(g, 'ret', 'market_cap')
    ).to_frame(name='market_return')
    mkt_weighted.to_csv(os.path.join(out_dir, 'market_return.csv'))
    
    print(f"ETF benchmark panel generation complete. Files saved to: {out_dir}")

if __name__ == "__main__":
    build_etfs()
