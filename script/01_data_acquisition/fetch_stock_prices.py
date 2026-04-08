import wrds
import pandas as pd
import os

def fetch_crsp_monthly():
    print("Attempting to connect to WRDS database (ensure credentials/config are correct)...")
    try:
        db = wrds.Connection()
        print("✅ Successfully connected to WRDS!")
    except Exception as e:
        print(f"❌ Connection to WRDS failed. Error: {e}")
        return

    # SQL Query: Pull from `crsp.msf` (CRSP Monthly Stock File)
    # Includes classic fields: permno, ticker, sic, price, return, shrout, vol.
    # JOIN with msenames to get time-varying tickers and SIC codes.
    # Calculate market_cap as |PRC| * shrout.
    query = """
        SELECT a.permno, b.ticker, b.siccd AS SIC, a.date, a.prc, a.ret, a.shrout, a.vol,
               (ABS(a.prc) * a.shrout) AS market_cap
        FROM crsp.msf AS a
        LEFT JOIN crsp.msenames AS b
          ON a.permno = b.permno
          AND a.date >= b.namedt
          AND a.date <= b.nameendt
        WHERE a.date >= '2012-01-01' 
          AND a.date <= '2023-12-31'
    """
    
    print("Querying and downloading monthly CRSP data for 2012-2023 (may take a few minutes)...")
    msf_df = db.raw_sql(query, date_cols=['date'])
    
    print(f"✅ Download complete! Obtained {msf_df.shape[0]} monthly observations.")
    print("Splitting data by stock into individual CSVs in raw_data (IO may take a few seconds)...")
    
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(BASE_DIR, '../../raw_data/stock_price')
    os.makedirs(output_dir, exist_ok=True)
    
    # Group by permno for maximum safety (tickers change over time)
    for permno, group_df in msf_df.groupby('permno'):
        # Use the latest available ticker for file naming
        valid_tickers = group_df['ticker'].dropna()
        ticker_name = valid_tickers.iloc[-1] if not valid_tickers.empty else "UNKNOWN"
        
        # Sanitize ticker for filesystem
        clean_ticker = str(ticker_name).replace('/', '-').replace('\\', '-')
        
        file_name = f"{clean_ticker}_{permno}.csv"
        file_path = os.path.join(output_dir, file_name)
        
        group_df.sort_values('date').to_csv(file_path, index=False)
        
    print(f"🎉 Splitting complete! {msf_df['permno'].nunique()} stocks saved to: {output_dir}")
    
    db.close()
    print("WRDS connection closed.")

if __name__ == "__main__":
    fetch_crsp_monthly()
