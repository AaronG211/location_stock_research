set -e
echo "Running clean_stock_prices.py..."
python3 02_cleaning_processing/clean_stock_prices.py
echo "Running merge_geo_stock.py..."
python3 02_cleaning_processing/merge_geo_stock.py
echo "Running build_monthly_zip_panel.py..."
python3 02_cleaning_processing/build_monthly_zip_panel.py
echo "Running clean_zip_panel.py..."
python3 02_cleaning_processing/clean_zip_panel.py
echo "Running build_msa_panel.py..."
python3 02_cleaning_processing/build_msa_panel.py
echo "Running build_etfs.py..."
python3 02_cleaning_processing/build_etfs.py
echo "Running add_benchmark_returns.py..."
python3 02_cleaning_processing/add_benchmark_returns.py
echo "Running final_clean_for_regression.py..."
python3 02_cleaning_processing/final_clean_for_regression.py
echo "Generating Summary Stats..."
Rscript 03_analysis/table1_summary_stats.R
echo "Running Model 1..."
Rscript 03_analysis/regress_betas.R
echo "Running Model 2..."
Rscript 03_analysis/regress_betas_model2_vw.R
echo "Pipeline successfully completed!"
