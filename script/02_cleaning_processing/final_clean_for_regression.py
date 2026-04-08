import os
import pandas as pd
import glob
import numpy as np

def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    stock_dir = os.path.join(BASE_DIR, '../../cleaned_data/stock_price')
    msa_panel_path = os.path.join(BASE_DIR, '../../cleaned_data/monthly_msa_panel_144mo.csv')
    
    print("Loading geographic MSA panel to calculate monthly listing counts per city...")
    msa_df = pd.read_csv(msa_panel_path, index_col=0)
    msa_df.index.name = 'Month'
    msa_long = msa_df.reset_index().melt(id_vars=['Month'], var_name='Ticker', value_name='Location_MSA')
    msa_long.dropna(subset=['Location_MSA'], inplace=True)
    msa_long['Location_MSA'] = msa_long['Location_MSA'].apply(lambda x: str(int(x)) if pd.notna(x) and str(x).replace('.','').isdigit() else x)
    
    # Build fast lookup dictionaries
    # 1. MSA location for a given stock/month
    msa_dict = msa_long.set_index(['Month', 'Ticker'])['Location_MSA'].to_dict()
    # 2. Total listing count for a given MSA/month
    loc_counts = msa_long.groupby(['Month', 'Location_MSA']).size().to_dict()

    csv_files = glob.glob(os.path.join(stock_dir, "*.csv"))
    
    print(f"Total stock files to process for final cleaning: {len(csv_files)}")
    deleted_files = 0
    kept_files = 0
    
    for i, file in enumerate(csv_files):
        try:
            df = pd.read_csv(file)
        except Exception as e:
            os.remove(file)
            deleted_files += 1
            continue

        ticker = os.path.basename(file).split('_')[0]
        # Generate standardized month for matching
        df['Month'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m')
        
        # Step 1: Remove rows (months) where the location has < 5 listings
        locs = [msa_dict.get((m, ticker)) for m in df['Month']]
        counts = [loc_counts.get((m, l), 0) if pd.notna(l) else 0 for m, l in zip(df['Month'], locs)]
        
        # Keep only segments where at least 5 firms coexist in the same city
        df_filtered = df[np.array(counts) >= 5].copy()
        
        # Step 2: Temporal continuity check
        # Require >= 24 months in each period (2012-2015, 2016-2019, 2020-2023)
        p1 = df_filtered[(df_filtered['Month'] >= '2012-01') & (df_filtered['Month'] <= '2015-12')]
        p2 = df_filtered[(df_filtered['Month'] >= '2016-01') & (df_filtered['Month'] <= '2019-12')]
        p3 = df_filtered[(df_filtered['Month'] >= '2020-01') & (df_filtered['Month'] <= '2023-12')]
        
        if len(p1) >= 24 and len(p2) >= 24 and len(p3) >= 24:
            # Overwrite and save after cleaning process
            df_filtered.drop(columns=['Month'], inplace=True)
            df_filtered.to_csv(file, index=False)
            kept_files += 1
        else:
            # 时间连续性不达标，或者数据大量缺失，整个股票被剔除并粉碎清理出文件夹
            os.remove(file)
            deleted_files += 1
            
        if i > 0 and i % 1500 == 0:
            print(f"Scanned {i} files... Kept: {kept_files}, Purged: {deleted_files}")

    print("="*60)
    print(f"Final pre-regression cleaning completed!")
    print(f"Total surviving stocks: {kept_files}")
    print(f"Total stocks purged due to sparsity/discontinuity: {deleted_files}")
    print("="*60)

if __name__ == "__main__":
    main()
