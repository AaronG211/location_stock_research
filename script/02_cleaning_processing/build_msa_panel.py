import pandas as pd
import numpy as np
import os

def build_msa_panel(zip_panel_path, crosswalk_path, output_path):
    print(f"Loading cleaned ZIP wide panel: {zip_panel_path}")
    zip_df = pd.read_csv(zip_panel_path, index_col=0, dtype=str)

    print(f"Reading HUD ZIP-to-CBSA crosswalk: {crosswalk_path}")
    mapping_df = pd.read_csv(crosswalk_path, dtype=str)

    # ------------------
    # Resolve one-to-many ZIP-to-CBSA mappings
    # ------------------
    # A ZIP code may span multiple CBSAs. For corporate headquarters, we pick
    # the unique CBSA with the highest business coverage ratio (BUS_RATIO).
    print("Resolving ZIP-to-CBSA ratios, selecting dominant CBSA by BUS_RATIO...")

    mapping_df['BUS_RATIO'] = pd.to_numeric(mapping_df['BUS_RATIO'], errors='coerce').fillna(0)

    mapping_df = mapping_df.sort_values(by=['ZIP', 'BUS_RATIO'], ascending=[True, False])
    mapping_unique = mapping_df.drop_duplicates(subset=['ZIP'], keep='first')

    # Final lookup dictionary: { '10001': '31080', ... }
    zip_to_cbsa_dict = dict(zip(mapping_unique['ZIP'].str.strip(), mapping_unique['CBSA'].str.strip()))

    # ------------------
    # Apply mapping across the full panel
    # ------------------
    print("Building lookup dictionary and applying full-table ZIP-to-CBSA conversion...")
    def get_msa(zip_code):
        if pd.isna(zip_code):
            return pd.NA
        zip_str = str(zip_code).strip()
        cbsa = zip_to_cbsa_dict.get(zip_str, pd.NA)

        # safely handle pd.NA
        if pd.isna(cbsa):
            return pd.NA

        # HUD uses 99999 to mark rural areas outside any CBSA; treat as missing
        if str(cbsa) == '99999':
            return pd.NA
        return cbsa

    # Pandas >= 2.1 recommends map over applymap
    msa_df = zip_df.map(get_msa)

    # Optional: drop columns with no valid MSA assignment (firms always in rural areas)
    # print("Dropping firms with no valid MSA...")
    # msa_df = msa_df.dropna(axis=1, how='all')

    # ------------------
    # Save results
    # ------------------
    print(f"Saving MSA panel ({msa_df.shape[0]} months x {msa_df.shape[1]} firms)...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    msa_df.to_csv(output_path)

    print(f"Conversion complete. MSA (CBSA) wide panel saved to: {output_path}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    INPUT_ZIP_PANEL = os.path.join(BASE_DIR, '../../cleaned_data/monthly_zip_panel_180mo_cleaned.csv')

    CROSSWALK_FILE = os.path.join(BASE_DIR, '../../raw_data/ZIP_MSA_122023.csv')

    OUTPUT_MSA_PANEL = os.path.join(BASE_DIR, '../../cleaned_data/monthly_msa_panel_180mo.csv')
    
    build_msa_panel(INPUT_ZIP_PANEL, CROSSWALK_FILE, OUTPUT_MSA_PANEL)
