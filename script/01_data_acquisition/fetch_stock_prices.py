import wrds
import pandas as pd
import os

DEFAULT_START_DATE = "2008-01-01"
DEFAULT_END_DATE = "2022-12-31"


def fetch_crsp_monthly(start_date=DEFAULT_START_DATE, end_date=DEFAULT_END_DATE):
    print("Attempting to connect to WRDS database (ensure credentials/config are correct)...")
    try:
        db = wrds.Connection()
        print("Successfully connected to WRDS!")
    except Exception as e:
        print(f"Connection to WRDS failed. Error: {e}")
        return

    # SQL Query: Pull from `crsp.msf` (CRSP Monthly Stock File).
    # This intentionally fetches a broad raw universe for the target window.
    # Common-stock screening is deferred to clean_stock_prices.py so the
    # acquisition stage remains wide and the cleaning stage handles sample rules.
    query = f"""
        SELECT a.permno, b.ticker, b.siccd AS SIC, b.shrcd, b.exchcd, a.date, a.prc, a.ret, a.shrout, a.vol,
               (ABS(a.prc) * a.shrout) AS market_cap
        FROM crsp.msf AS a
        LEFT JOIN crsp.msenames AS b
          ON a.permno = b.permno
          AND a.date >= b.namedt
          AND a.date <= b.nameendt
        WHERE a.date >= '{start_date}' 
          AND a.date <= '{end_date}'
    """
    
    print(
        f"Querying and downloading broad monthly CRSP data for {start_date} to "
        f"{end_date} (may take a few minutes)..."
    )
    msf_df = db.raw_sql(query, date_cols=['date'])
    
    print(f"Download complete! Obtained {msf_df.shape[0]} monthly observations.")
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
        
    print(f"Splitting complete! {msf_df['permno'].nunique()} stocks saved to: {output_dir}")
    
    db.close()
    print("WRDS connection closed.")

if __name__ == "__main__":
    fetch_crsp_monthly()
