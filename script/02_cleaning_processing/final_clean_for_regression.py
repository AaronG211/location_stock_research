"""Final filtering gate before running regressions.

All sample-construction filters (MSA >=5 firms AND >=2 industries) are now
applied upstream in add_benchmark_returns.py. This script only enforces:
  1. Drop firm-months with any NA in the baseline Pirinsky-Wang Model 1 inputs
     (ret, location_simple, market_return, industry_simple). Rows where
     location_simple is NaN
     correspond to invalid MSAs, so this implicitly drops them.
  2. Purge stocks that don't have >=24 months of CLEAN data in ANY of the
     three 5-year sub-periods (2008-2012, 2013-2017, 2018-2022).
"""
import os
import pandas as pd
import glob

REQUIRED_COLS = [
    'ret',
    'location_simple',
    'market_return',
    'industry_simple',
]
MIN_MONTHS_PER_PERIOD = 24

SUBPERIODS = [
    ('2008-01', '2012-12'),
    ('2013-01', '2017-12'),
    ('2018-01', '2022-12'),
]


def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    stock_dir = os.path.join(BASE_DIR, '../../cleaned_data/stock_price')

    csv_files = glob.glob(os.path.join(stock_dir, "*.csv"))
    print(f"Total stock files to process for final cleaning: {len(csv_files)}")

    deleted_files = 0
    kept_files = 0
    rows_dropped_total = 0

    for i, file in enumerate(csv_files):
        try:
            df = pd.read_csv(file)
        except Exception:
            os.remove(file)
            deleted_files += 1
            continue

        if df.empty:
            os.remove(file)
            deleted_files += 1
            continue

        # Ensure all required regression inputs are present as columns
        missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing_cols:
            os.remove(file)
            deleted_files += 1
            continue

        rows_before = len(df)

        # Step 1: Drop rows with any NA in the required regression columns.
        # location_simple = NaN means the stock was in an invalid MSA that
        # month (MSA had <5 firms or <2 industries), so it gets excluded here.
        df_clean = df.dropna(subset=REQUIRED_COLS).copy()
        rows_dropped_total += rows_before - len(df_clean)

        if df_clean.empty:
            os.remove(file)
            deleted_files += 1
            continue

        # Step 2: Temporal continuity check across the three 5-year windows.
        # Keep the stock if it has >=24 clean months in AT LEAST ONE window;
        # otherwise it can't support a reliable time-series beta anywhere.
        df_clean['Month'] = pd.to_datetime(df_clean['date']).dt.strftime('%Y-%m')

        survives_any_period = False
        for start, end in SUBPERIODS:
            n = ((df_clean['Month'] >= start) & (df_clean['Month'] <= end)).sum()
            if n >= MIN_MONTHS_PER_PERIOD:
                survives_any_period = True
                break

        if not survives_any_period:
            os.remove(file)
            deleted_files += 1
            continue

        df_clean.drop(columns=['Month'], inplace=True)
        df_clean.to_csv(file, index=False)
        kept_files += 1

        if i > 0 and i % 1500 == 0:
            print(f"Scanned {i} files... Kept: {kept_files}, Purged: {deleted_files}")

    print("=" * 60)
    print("Final pre-regression cleaning completed.")
    print(f"  Surviving stocks: {kept_files}")
    print(f"  Purged stocks:    {deleted_files}")
    print(f"  Total firm-month rows dropped due to NA: {rows_dropped_total:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
