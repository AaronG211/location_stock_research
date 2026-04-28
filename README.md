# Does Corporate Headquarters Location Still Matter for Stock Returns?
### A Modern Replication and Extension of Pirinsky and Wang (2006)

This repository replicates Pirinsky and Wang (2006) using U.S. equity data from 2008 to 2022. The original paper documents that stocks of firms headquartered in the same metropolitan area exhibit return co-movement beyond what industry and market factors can explain. This study tests whether the effect persists in the modern era, and introduces a value-weighted extension (Model 2) alongside the original equal-weighted specification (Model 1).

For replication, refer to the project_log.txt and code_explaination.txt (both in the documentation folder) would be helpful. 

---

## Repository Structure

```
research/
├── raw_data/                        # Raw inputs (WRDS downloads + manual files)
│   ├── stock_price/                 # One CSV per PERMNO from CRSP
│   ├── cik_ticker_permno_mapping.csv
│   ├── sp500_annual_constituents_2007_2022.csv
│   ├── monthly_market_factors.csv
│   ├── ZIP_MSA_122023.csv           # HUD ZIP-CBSA crosswalk (manual download)
│   └── LoughranMcDonald_10-K_HeaderData_1993-2024.csv  # (manual download)
│
├── cleaned_data/                    # Outputs from the cleaning pipeline
│   ├── stock_price/                 # Cleaned stock files with index returns added
│   ├── etfs/                        # Location, industry, and market index series
│   ├── LM_10K_Headers_Geo_Filtered.csv
│   ├── merged_geo_ticker_data.csv
│   ├── monthly_zip_panel_180mo.csv
│   ├── monthly_zip_panel_180mo_cleaned.csv
│   ├── monthly_msa_panel_180mo.csv
│   └── msa_validity.csv
│
├── script/
│   ├── 01_data_acquisition/         # WRDS data fetching + LM header filtering
│   ├── 02_cleaning_processing/      # Cleaning, geographic assignment, index construction
│   ├── 03_analysis/                 # R regression scripts and summary stats
│   ├── run_pipeline.sh              # Runs Stages 2–3 end-to-end
│   └── run_pipeline2.sh             # Runs from build_etfs onwards only
│
├── documentation/
│   ├── second_draft.Rmd             # Main paper (current draft)
│   ├── second_draft.pdf             # Compiled PDF
│   ├── changes_highlighted.pdf      # Diff between first and second draft
│   ├── first_draft_new.Rmd          # First draft (baseline for diff)
│   ├── project_log.txt              # Full replication guide with step-by-step instructions
│   └── Does Corporate Headquarters Location Matter for Stock Returns-.pdf  # Original paper
│
└── README.md
```

---

## Data Requirements

The following files must be **manually downloaded** before running the pipeline:

| File | Source |
|------|--------|
| `LoughranMcDonald_10-K_HeaderData_1993-2024.csv` | https://sraf.nd.edu/sec-edgar-data/ |
| `ZIP_MSA_122023.csv` | https://www.huduser.gov/portal/datasets/usps_crosswalk.html (Dec 2023 vintage) |
| FF48 industry classification | https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html |

Scripts in `01_data_acquisition/` that fetch CRSP and Compustat data require a **WRDS account** with access to CRSP and Compustat.

---

## How to Replicate

A full step-by-step guide (including input/output file names for each script) is in `documentation/project_log.txt`.

**Quick start** — if raw data is already in place, run from the `script/` directory:

```bash
bash run_pipeline.sh
```

This executes the full cleaning pipeline (Stage 2) and all regression analyses (Stage 3) in sequence.

---

## Key Outputs

| Output | Location |
|--------|----------|
| Final paper (PDF) | `documentation/second_draft.pdf` |
| Model 1 regression results | `script/regress_results_ew.txt` |
| Model 2 regression results | `script/model2_results_ew.txt` |
| Summary stats tables | `script/03_analysis/table1_panel_a.csv`, `table1_panel_b.csv` |
| First vs. second draft diff | `documentation/changes_highlighted.pdf` |

---

## Dependencies

**Python** (≥ 3.8)
```
pandas
numpy
wrds
```

**R** (≥ 4.0)
```
data.table
knitr
kableExtra
ggplot2
patchwork
```
