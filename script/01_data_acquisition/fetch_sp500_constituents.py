import os
import wrds
import pandas as pd

DEFAULT_START_YEAR = 2007
DEFAULT_END_YEAR = 2022


def fetch_annual_sp500_constituents(start_year=DEFAULT_START_YEAR, end_year=DEFAULT_END_YEAR):
    print("Connecting to WRDS...")
    try:
        db = wrds.Connection()
    except Exception as e:
        print(f"Connection to WRDS failed. Error: {e}")
        return

    print("Downloading S&P 500 historical constituent list from CRSP...")
    # 'ending' is often missing or very large if the stock is still active in the index
    sp500_query = """
        SELECT permno, start, ending
        FROM crsp.msp500list
    """
    sp500 = db.raw_sql(sp500_query, date_cols=['start', 'ending'])
    
    # Fill missing 'ending' with a far future date
    sp500['ending'] = sp500['ending'].fillna(pd.to_datetime('2099-12-31'))
    
    print("Downloading CRSP stocknames for precise ticker mapping...")
    names_query = """
        SELECT permno, ticker, comnam, namedt, nameenddt
        FROM crsp.stocknames
    """
    names = db.raw_sql(names_query, date_cols=['namedt', 'nameenddt'])
    names['nameenddt'] = names['nameenddt'].fillna(pd.to_datetime('2099-12-31'))
    
    db.close()
    print("WRDS connection closed.")
    
    print(f"Building annual snapshots for {start_year}-{end_year}...")
    
    # Generate end-of-year dates to represent the annual constituency
    years = range(start_year, end_year + 1)
    snapshots = []
    
    for year in years:
        # We take December 31st of each year as the snapshot benchmark
        snapshot_date = pd.to_datetime(f"{year}-12-31")
        
        # 1. Filter SP500 list for stocks active in the S&P 500 on this specific date
        active_sp500 = sp500[(sp500['start'] <= snapshot_date) & (sp500['ending'] >= snapshot_date)].copy()
        active_sp500['year'] = year
        
        # 2. Merge with stocknames to get the ticker ON this specific date
        merged = pd.merge(active_sp500, names, on='permno', how='left')
        
        # 3. Filter for the exact company name/ticker active on the snapshot date
        mask = (merged['namedt'] <= snapshot_date) & (merged['nameenddt'] >= snapshot_date)
        valid_names = merged[mask].copy()
        
        # 4. Drop any potential duplicates caused by intraday ticker changes
        valid_names = valid_names.drop_duplicates(subset=['permno'], keep='last')
        
        snapshots.append(valid_names[['year', 'permno', 'ticker', 'comnam']])
        
    master_panel = pd.concat(snapshots, ignore_index=True)
    master_panel = master_panel.sort_values(['year', 'ticker'])
    
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    output_name = f"sp500_annual_constituents_{start_year}_{end_year}.csv"
    OUTPUT_FILE = os.path.abspath(os.path.join(BASE_DIR, f'../../raw_data/{output_name}'))
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    master_panel.to_csv(OUTPUT_FILE, index=False)
    
    print(f"Successfully exported S&P 500 annual constituents ({start_year}-{end_year})!")
    print(f"Total records captured: {len(master_panel)}")
    print(f"Output saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    fetch_annual_sp500_constituents()
