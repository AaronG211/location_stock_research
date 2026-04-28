import glob
import os

import numpy as np
import pandas as pd

# Pirinsky-Wang area screen
MIN_FIRMS_PER_MSA = 5
MIN_INDUSTRIES_PER_MSA = 2


def normalize_permno(value):
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    stock_dir = os.path.abspath(os.path.join(base_dir, "../../cleaned_data/stock_price"))
    msa_panel_path = os.path.abspath(os.path.join(base_dir, "../../cleaned_data/monthly_msa_panel_180mo.csv"))
    validity_out = os.path.abspath(os.path.join(base_dir, "../../cleaned_data/msa_validity_panel.csv"))

    print("Loading geographic MSA panel...")
    msa_df = pd.read_csv(msa_panel_path, index_col=0, dtype=str)
    msa_df.index.name = "Month"
    msa_df.columns = [normalize_permno(col) for col in msa_df.columns]
    eligible_permnos = set(msa_df.columns)

    msa_long = msa_df.reset_index().melt(
        id_vars=["Month"], var_name="permno", value_name="Location_MSA"
    )
    msa_long["permno"] = msa_long["permno"].map(normalize_permno)
    msa_long.dropna(subset=["Location_MSA"], inplace=True)
    msa_long["Location_MSA"] = msa_long["Location_MSA"].apply(
        lambda x: str(int(x)) if pd.notna(x) and str(x).replace(".", "").isdigit() else x
    )

    print("Pass 1: Reading all eligible stock data and assembling the replication universe...")
    csv_files = glob.glob(os.path.join(stock_dir, "*.csv"))
    all_stocks = []

    for file in csv_files:
        permno = normalize_permno(os.path.basename(file).split("_")[1].split(".")[0])
        if permno not in eligible_permnos:
            continue

        df = pd.read_csv(file)
        if df.empty or "ret" not in df.columns or "market_cap" not in df.columns:
            continue

        df["Month"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
        df["permno"] = permno
        if "industry" not in df.columns:
            df["industry"] = pd.NA

        df_subset = df[["Month", "permno", "ret", "market_cap", "industry"]].copy()
        df_subset["ret"] = pd.to_numeric(df_subset["ret"], errors="coerce")
        df_subset["market_cap"] = pd.to_numeric(df_subset["market_cap"], errors="coerce")
        df_subset.dropna(subset=["ret", "market_cap"], inplace=True)
        all_stocks.append(df_subset)

    if not all_stocks:
        raise RuntimeError("No eligible stock files were loaded for benchmark construction.")

    master_df = pd.concat(all_stocks, ignore_index=True)
    master_df = pd.merge(master_df, msa_long, on=["Month", "permno"], how="left")
    master_df["wret"] = master_df["ret"] * master_df["market_cap"]

    # Sample-derived market proxy: value-weighted return of the eligible stock universe.
    market_agg = (
        master_df.groupby("Month")
        .agg(
            market_sum_wret=("wret", "sum"),
            market_sum_cap=("market_cap", "sum"),
        )
        .reset_index()
    )
    market_agg["market_return"] = market_agg["market_sum_wret"] / market_agg["market_sum_cap"]
    market_raw_dict = dict(zip(market_agg["Month"], market_agg["market_return"]))

    print(
        f"Computing MSA validity (>={MIN_FIRMS_PER_MSA} firms AND >={MIN_INDUSTRIES_PER_MSA} industries per month)..."
    )
    msa_with_loc = master_df.dropna(subset=["Location_MSA"])
    msa_validity = (
        msa_with_loc.groupby(["Month", "Location_MSA"])
        .agg(n_firms=("permno", "nunique"))
        .reset_index()
    )

    ind_counts = (
        msa_with_loc[msa_with_loc["industry"] != "Other"]
        .groupby(["Month", "Location_MSA"])
        .agg(n_industries=("industry", "nunique"))
        .reset_index()
    )
    msa_validity = msa_validity.merge(
        ind_counts, on=["Month", "Location_MSA"], how="left"
    ).fillna(0)
    valid_mask = (
        (msa_validity["n_firms"] >= MIN_FIRMS_PER_MSA)
        & (msa_validity["n_industries"] >= MIN_INDUSTRIES_PER_MSA)
    )
    msa_validity["is_valid"] = valid_mask

    os.makedirs(os.path.dirname(validity_out), exist_ok=True)
    msa_validity.to_csv(validity_out, index=False)

    valid_keys_df = msa_validity.loc[valid_mask, ["Month", "Location_MSA"]]

    print("Building leave-one-out industry and location aggregates on raw returns...")
    ind_agg = (
        master_df.dropna(subset=["industry"])
        .groupby(["Month", "industry"])
        .agg(
            ind_sum_ret=("ret", "sum"),
            ind_count=("ret", "count"),
            ind_sum_wret=("wret", "sum"),
            ind_sum_cap=("market_cap", "sum"),
        )
        .reset_index()
    )

    master_loc_valid = msa_with_loc.merge(
        valid_keys_df, on=["Month", "Location_MSA"], how="inner"
    )
    loc_agg = (
        master_loc_valid.groupby(["Month", "Location_MSA"])
        .agg(
            loc_sum_ret=("ret", "sum"),
            loc_count=("ret", "count"),
            loc_sum_wret=("wret", "sum"),
            loc_sum_cap=("market_cap", "sum"),
        )
        .reset_index()
    )

    ind_agg_dict = ind_agg.set_index(["Month", "industry"]).to_dict("index")
    loc_agg_dict = loc_agg.set_index(["Month", "Location_MSA"]).to_dict("index")
    msa_dict = msa_long.set_index(["Month", "permno"])["Location_MSA"].to_dict()

    def get_ind_simple(row):
        agg = ind_agg_dict.get((row["Month"], row["industry"]))
        if agg and agg["ind_count"] > 1:
            return (agg["ind_sum_ret"] - row["ret"]) / (agg["ind_count"] - 1)
        return np.nan

    def get_ind_weighted(row):
        agg = ind_agg_dict.get((row["Month"], row["industry"]))
        if agg:
            loo_cap = agg["ind_sum_cap"] - row["market_cap"]
            if loo_cap > 0:
                return (agg["ind_sum_wret"] - row["ret"] * row["market_cap"]) / loo_cap
        return np.nan

    def get_loc_simple(row, permno):
        loc = msa_dict.get((row["Month"], permno))
        agg = loc_agg_dict.get((row["Month"], loc))
        if agg and agg["loc_count"] > 1:
            return (agg["loc_sum_ret"] - row["ret"]) / (agg["loc_count"] - 1)
        return np.nan

    def get_loc_weighted(row, permno):
        loc = msa_dict.get((row["Month"], permno))
        agg = loc_agg_dict.get((row["Month"], loc))
        if agg:
            loo_cap = agg["loc_sum_cap"] - row["market_cap"]
            if loo_cap > 0:
                return (agg["loc_sum_wret"] - row["ret"] * row["market_cap"]) / loo_cap
        return np.nan

    print("Pass 2: Injecting excess-return benchmarks into individual stock CSVs...")
    for i, file in enumerate(csv_files):
        permno = normalize_permno(os.path.basename(file).split("_")[1].split(".")[0])
        df = pd.read_csv(file)
        if df.empty or "ret" not in df.columns or permno not in eligible_permnos:
            continue

        # Drop stale benchmark columns before rebuilding the regression inputs.
        stale_cols = [
            "rf",
            "ret_excess",
            "market_return",
            "market_excess",
            "industry_simple",
            "industry_weighted",
            "location_simple",
            "location_weighted",
            "location_return",
            "industry_simple_excess",
            "industry_weighted_excess",
            "location_simple_excess",
            "location_weighted_excess",
        ]
        for col in stale_cols:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        df["Month"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
        df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
        df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
        df["market_return"] = df["Month"].map(market_raw_dict)
        df["industry_simple"] = df.apply(get_ind_simple, axis=1)
        df["industry_weighted"] = df.apply(get_ind_weighted, axis=1)
        df["location_simple"] = df.apply(lambda row: get_loc_simple(row, permno), axis=1)
        df["location_weighted"] = df.apply(lambda row: get_loc_weighted(row, permno), axis=1)
        df["location_return"] = df["location_weighted"]

        df.drop(columns=["Month"], inplace=True)
        df.to_csv(file, index=False)

        if i > 0 and i % 2500 == 0:
            print(f"Progress: {i} / {len(csv_files)} stock files updated...")

    print("Success! Stock CSVs now contain raw-return benchmarks built on the eligible replication universe.")


if __name__ == "__main__":
    main()
