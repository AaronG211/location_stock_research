import os
import pandas as pd
import glob
import numpy as np

def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    stock_dir = os.path.abspath(os.path.join(BASE_DIR, '../../cleaned_data/stock_price'))
    msa_panel_path = os.path.abspath(os.path.join(BASE_DIR, '../../cleaned_data/monthly_msa_panel_144mo.csv'))
    mkt_path = os.path.abspath(os.path.join(BASE_DIR, '../../cleaned_data/etf_price/market_weighted.csv'))
    
    print("Loading geographic MSA panel and market weighted returns...")
    # 1. Prepare MSA long panel for fast lookup
    msa_df = pd.read_csv(msa_panel_path, index_col=0)
    msa_df.index.name = 'Month'
    msa_long = msa_df.reset_index().melt(id_vars=['Month'], var_name='Ticker', value_name='Location_MSA')
    msa_long.dropna(subset=['Location_MSA'], inplace=True)
    msa_long['Location_MSA'] = msa_long['Location_MSA'].apply(lambda x: str(int(x)) if pd.notna(x) and str(x).replace('.','').isdigit() else x)
    
    # 2. Build dictionary for Market_Weighted_Return
    mkt_df = pd.read_csv(mkt_path)
    mkt_dict = dict(zip(mkt_df['Month'], mkt_df['Market_Weighted_Return']))

    print("Pass 1: Reading all stock data and calculating aggregate market and industry pools...")
    csv_files = glob.glob(os.path.join(stock_dir, "*.csv"))
    
    all_stocks = []
    for file in csv_files:
        df = pd.read_csv(file)
        if df.empty or 'ret' not in df.columns or 'market_cap' not in df.columns:
            continue
            
        ticker = os.path.basename(file).split('_')[0]
        df['Month'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m')
        df['Ticker'] = ticker
        
        df_subset = df[['Month', 'Ticker', 'ret', 'market_cap', 'industry']].copy()
        df_subset['ret'] = pd.to_numeric(df_subset['ret'], errors='coerce')
        df_subset['market_cap'] = pd.to_numeric(df_subset['market_cap'], errors='coerce')
        df_subset.dropna(subset=['ret', 'market_cap'], inplace=True)
        
        all_stocks.append(df_subset)
        
    master_df = pd.concat(all_stocks, ignore_index=True)
    
    # Merge Location
    master_df = pd.merge(master_df, msa_long, on=['Month', 'Ticker'], how='left')
    
    # preparation for Leave-One-Out (LOO) calculations
    master_df['wret'] = master_df['ret'] * master_df['market_cap']
    
    # Industry Aggregation: Sum of weighted returns and total market cap per month/industry
    ind_agg = master_df.dropna(subset=['industry']).groupby(['Month', 'industry']).agg(
        ind_sum_ret=('ret', 'sum'),
        ind_count=('ret', 'count'),
        ind_sum_wret=('wret', 'sum'),
        ind_sum_cap=('market_cap', 'sum')
    ).reset_index()
    
    # Location Aggregation: Sum of value-weighted returns and total market cap per month/MSA
    loc_agg = master_df.dropna(subset=['Location_MSA']).groupby(['Month', 'Location_MSA']).agg(
        loc_sum_wret=('wret', 'sum'),
        loc_sum_cap=('market_cap', 'sum')
    ).reset_index()

    print("Building lookup hash tables for O(1) retrieval...")
    ind_agg_dict = ind_agg.set_index(['Month', 'industry']).to_dict('index')
    loc_agg_dict = loc_agg.set_index(['Month', 'Location_MSA']).to_dict('index')
    msa_dict = msa_long.set_index(['Month', 'Ticker'])['Location_MSA'].to_dict()

    print("Pass 2: Calculating Leave-One-Out (LOO) benchmarks and injecting into individual CSVs...")
    
    def get_ind_loo(row):
        m = row['Month']
        ind = row['industry']
        if pd.isna(ind): return np.nan
        agg = ind_agg_dict.get((m, ind))
        if agg and agg['ind_count'] > 1:
            # Simple average excluding the stock itself
            return (agg['ind_sum_ret'] - row['ret']) / (agg['ind_count'] - 1)
        return np.nan

    def get_ind_weighted_loo(row):
        m = row['Month']
        ind = row['industry']
        if pd.isna(ind): return np.nan
        agg = ind_agg_dict.get((m, ind))
        if agg:
            my_wret = row['ret'] * row['market_cap']
            my_cap = row['market_cap']
            loo_cap = agg['ind_sum_cap'] - my_cap
            if loo_cap > 1:
                # Value-weighted average excluding the stock itself
                return (agg['ind_sum_wret'] - my_wret) / loo_cap
        return np.nan

    def get_loc_loo(row, ticker):
        m = row['Month']
        loc = msa_dict.get((m, ticker))
        if pd.isna(loc): return np.nan
        
        agg = loc_agg_dict.get((m, loc))
        if agg:
            my_wret = row['ret'] * row['market_cap']
            my_cap = row['market_cap']
            loo_cap = agg['loc_sum_cap'] - my_cap
            # Ensure remaining market cap is significant (>1$)
            if loo_cap > 1:
                # Weighted average excluding the stock itself
                return (agg['loc_sum_wret'] - my_wret) / loo_cap
        return np.nan

    for i, file in enumerate(csv_files):
        df = pd.read_csv(file)
        if df.empty or 'ret' not in df.columns:
            continue
            
        # Remove existing columns if present to avoid duplication
        for col in ['market_return', 'industry_simple', 'location_return']:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
                
        ticker = os.path.basename(file).split('_')[0]
        
        df['Month'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m')
        
        # 1. Inject total market returns
        df['market_return'] = df['Month'].map(mkt_dict)
        
        # 2. Inject Leave-One-Out Industry and Location benchmarks
        df['industry_simple'] = df.apply(get_ind_loo, axis=1)
        df['industry_weighted'] = df.apply(get_ind_weighted_loo, axis=1)
        df['location_return'] = df.apply(lambda r: get_loc_loo(r, ticker), axis=1)
        
        df.drop(columns=['Month'], inplace=True)
        df.to_csv(file, index=False)
        
        if i % 2500 == 0 and i > 0:
            print(f"Progress: {i} / {len(csv_files)} stock files updated...")

    print("🎉 Success! All stock CSVs updated with 'market_return', 'industry_simple', and 'location_return' (LOO) attributes.")

if __name__ == "__main__":
    main()
