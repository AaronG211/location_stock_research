import os

import pandas as pd
import wrds

DEFAULT_START_DATE = "2008-01-01"
DEFAULT_END_DATE = "2022-12-31"


def _find_ff_monthly_table(db):
    candidates = db.raw_sql(
        """
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE column_name = 'rf'
          AND table_name LIKE '%factors_monthly%'
        ORDER BY table_schema, table_name
        """
    )
    if candidates.empty:
        return None

    preferred = candidates[
        (candidates["table_schema"].str.lower() == "ff")
        & (candidates["table_name"].str.lower() == "factors_monthly")
    ]
    row = preferred.iloc[0] if not preferred.empty else candidates.iloc[0]
    return f"{row['table_schema']}.{row['table_name']}"


def _normalize_rf(series):
    rf = pd.to_numeric(series, errors="coerce")
    # FF monthly RF is typically expressed in percent.
    if rf.dropna().abs().max() > 1:
        rf = rf / 100.0
    return rf


def fetch_market_factors(start_date=DEFAULT_START_DATE, end_date=DEFAULT_END_DATE):
    print("Connecting to WRDS...")
    db = wrds.Connection()

    print("Downloading CRSP monthly value-weighted market return...")
    market = db.raw_sql(
        f"""
        SELECT date, vwretd AS market_return
        FROM crsp.msi
        WHERE date >= '{start_date}'
          AND date <= '{end_date}'
        ORDER BY date
        """,
        date_cols=["date"],
    )

    ff_table = _find_ff_monthly_table(db)
    if ff_table is None:
        db.close()
        raise RuntimeError("Could not locate a WRDS monthly factor table with RF.")

    print(f"Downloading monthly risk-free rate from {ff_table}...")
    rf = db.raw_sql(
        f"""
        SELECT date, rf
        FROM {ff_table}
        WHERE date >= '{start_date}'
          AND date <= '{end_date}'
        ORDER BY date
        """,
        date_cols=["date"],
    )

    db.close()

    factors = pd.merge(market, rf, on="date", how="inner")
    factors["rf"] = _normalize_rf(factors["rf"])
    factors["market_return"] = pd.to_numeric(factors["market_return"], errors="coerce")
    factors["market_excess"] = factors["market_return"] - factors["rf"]
    factors["Month"] = factors["date"].dt.strftime("%Y-%m")
    factors = factors[["date", "Month", "market_return", "rf", "market_excess"]]

    base_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.abspath(os.path.join(base_dir, "../../raw_data"))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "monthly_market_factors.csv")
    factors.to_csv(out_path, index=False)

    print(f"Saved {len(factors)} monthly observations to {out_path}")
    return factors


if __name__ == "__main__":
    fetch_market_factors()
