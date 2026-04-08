import pandas as pd
import wrds

def fetch_cik_mapping():
    print("Connecting to WRDS...")
    db = wrds.Connection()
    
    print("Downloading Compustat company info (Mapping GVKEY to CIK)...")
    # Fetch Compustat company table
    comp = db.raw_sql("""
        SELECT gvkey, cik, conm
        FROM comp.company
    """)
    
    # Drop rows without CIK
    comp = comp.dropna(subset=['cik'])
    
    print("Downloading CRSP/Compustat Merged (CCM) linktable (Mapping GVKEY to PERMNO)...")
    # Fetch CCM linktable
    ccm = db.raw_sql("""
        SELECT gvkey, lpermno AS permno, linkdt, linkenddt, linktype, linkprim
        FROM crsp.ccmxpf_linktable
        WHERE linktype IN ('LU', 'LC')
        AND linkprim IN ('P', 'C')
    """)
    
    print("Downloading CRSP stocknames (Mapping PERMNO to Ticker)...")
    # Fetch CRSP stocknames to get the latest ticker symbol
    stocknames = db.raw_sql("""
        SELECT permno, ticker
        FROM crsp.stocknames
        ORDER BY permno, nameenddt
    """)
    stocknames = stocknames.dropna(subset=['ticker'])
    stocknames = stocknames.drop_duplicates(subset=['permno'], keep='last')
    
    print("Merging and cleaning mapping tables...")
    # Merge the two tables on gvkey
    mapping = pd.merge(comp, ccm, on='gvkey', how='inner')
    
    # Merge with ticker based on permno
    mapping = pd.merge(mapping, stocknames, on='permno', how='left')
    
    # Clean up the output
    mapping['cik'] = mapping['cik'].astype(str).str.zfill(10) # Format CIK as 10-digit string, left-padding with zeros
    
    # Fill NA in linkenddt with a far future date
    mapping['linkenddt'] = mapping['linkenddt'].fillna('2099-12-31')
    
    # Sort and structure
    mapping = mapping[['cik', 'ticker', 'permno', 'gvkey', 'conm', 'linkdt', 'linkenddt']]
    
    # Adjust output path for new folder structure (script/data_processing/)
    output_path = '../../raw_data/cik_ticker_permno_mapping.csv'
    mapping.to_csv(output_path, index=False)
    
    print(f"\nMapping table extraction complete! Total records: {len(mapping)}")
    print(f"File saved to: {output_path}")
    
    return mapping

if __name__ == "__main__":
    mapping_df = fetch_cik_mapping()
    print("\nFirst 5 rows of mapping table:")
    print(mapping_df.head())
