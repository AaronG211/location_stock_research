# Base R Beta Time Series Regression Script
# Model 1: R_it = alpha + beta_loc*RLOC + beta_mkt*MKT + beta_ind*RIND_ew
#
# NOTE: All sample filters (MSA >=5 firms, MSA >=2 industries, NA removal,
# minimum data coverage) are now applied upstream in 02_cleaning_processing/.
# This script assumes the stock CSVs have already been sanitized.

BASE_DIR  <- normalizePath("..")
stock_dir <- file.path(BASE_DIR, "cleaned_data/stock_price")
files <- list.files(stock_dir, pattern = "\\.csv$", full.names = TRUE)

cat("============================================================\n")
cat(sprintf("Initializing time-series regressions for %d surviving stocks...\n", length(files)))
cat("============================================================\n")


# Load SP500 annual constituents to construct Ex-ante universes for filtering
sp500 <- read.csv(file.path(BASE_DIR, "raw_data/sp500_annual_constituents_2007_2022.csv"), stringsAsFactors=FALSE)
sp_univ_p1 <- sp500$permno[sp500$year == 2007]  # Universe for 2008-2012
sp_univ_p2 <- sp500$permno[sp500$year == 2012]  # Universe for 2013-2017
sp_univ_p3 <- sp500$permno[sp500$year == 2017]  # Universe for 2018-2022

list_p1 <- list()
list_p2 <- list()
list_p3 <- list()

for (i in seq_along(files)) {
  f <- files[i]
  df <- tryCatch(read.csv(f, stringsAsFactors=FALSE), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0) next

  df$date <- as.Date(df$date)

  p1 <- df[df$date >= as.Date('2008-01-01') & df$date <= as.Date('2012-12-31'), ]
  p2 <- df[df$date >= as.Date('2013-01-01') & df$date <= as.Date('2017-12-31'), ]
  p3 <- df[df$date >= as.Date('2018-01-01') & df$date <= as.Date('2022-12-31'), ]
  
  run_reg <- function(data) {
    data <- data[complete.cases(data[, c(
      "ret",
      "location_simple",
      "market_return",
      "industry_simple"
    )]), ]
    if (nrow(data) >= 24) {
      # Use tryCatch to avoid stopping if collinearity causes failure
      model <- tryCatch(
        lm(
          ret ~ location_simple + market_return + industry_simple,
          data = data
        ),
        error = function(e) NULL
      )
      if (!is.null(model)) {
        coefs <- coef(model)
        # Check if all four coefficients (Intercept + 3 variables) are validly fitted
        if (length(coefs) == 4 && all(!is.na(coefs))) {
          return(data.frame(
            permno = data$permno[1],
            beta_loc = coefs["location_simple"],
            beta_mkt = coefs["market_return"],
            beta_ind = coefs["industry_simple"]
          ))
        }
      }
    }
    return(NULL)
  }
  
  if (!is.null(p1)) {
    res1 <- run_reg(p1)
    if (!is.null(res1)) list_p1[[length(list_p1) + 1]] <- res1
  }
  if (!is.null(p2)) {
    res2 <- run_reg(p2)
    if (!is.null(res2)) list_p2[[length(list_p2) + 1]] <- res2
  }
  if (!is.null(p3)) {
    res3 <- run_reg(p3)
    if (!is.null(res3)) list_p3[[length(list_p3) + 1]] <- res3
  }
}

# Aggregate all individual time-series betas
results_p1 <- do.call(rbind, list_p1)
results_p2 <- do.call(rbind, list_p2)
results_p3 <- do.call(rbind, list_p3)

summarize_betas <- function(res, period_name, sp_filter = NULL) {
  if (is.null(res) || nrow(res) == 0) return()
  
  if (!is.null(sp_filter)) {
    res <- res[res$permno %in% sp_filter, ]
  }
  
  if (nrow(res) == 0) {
      cat(sprintf("%-15s       None       None       None\n", period_name))
      return()
  }
  
  n <- nrow(res)
  
  # Calculate cross-sectional means of betas
  mean_loc = mean(res$beta_loc)
  mean_mkt = mean(res$beta_mkt)
  mean_ind = mean(res$beta_ind)
  
  # Calculate Fama-MacBeth style simple t-statistics: Mean / (StdDev / sqrt(N))
  t_loc = mean_loc / (sd(res$beta_loc) / sqrt(n))
  t_mkt = mean_mkt / (sd(res$beta_mkt) / sqrt(n))
  t_ind = mean_ind / (sd(res$beta_ind) / sqrt(n))
  
  # Output cleanly formatted like the provided image table
  cat(sprintf("%-15s %10.3f %10.3f %10.3f\n", period_name, mean_loc, mean_mkt, mean_ind))
  cat(sprintf("%-15s %10.2f %10.2f %10.2f\n", paste0("t-stat (N=", n, ")"), t_loc, t_mkt, t_ind))
}

cat("\n============================================================\n")
cat("          Results of Cross-Sectional Average Betas (ALL STOCKS)\n")
cat("============================================================\n")
cat(sprintf("%-15s %10s %10s %10s\n", "Period", "Beta_Loc", "Beta_Mkt", "Beta_Ind"))
cat("------------------------------------------------------------\n")
summarize_betas(results_p1, "2008-2012")
summarize_betas(results_p2, "2013-2017")
summarize_betas(results_p3, "2018-2022")
cat("============================================================\n")

cat("\n============================================================\n")
cat("          Results of Cross-Sectional Average Betas (SPY ONLY)\n")
cat("============================================================\n")
cat(sprintf("%-15s %10s %10s %10s\n", "Period", "Beta_Loc", "Beta_Mkt", "Beta_Ind"))
cat("------------------------------------------------------------\n")
summarize_betas(results_p1, "2008-2012", sp_univ_p1)
summarize_betas(results_p2, "2013-2017", sp_univ_p2)
summarize_betas(results_p3, "2018-2022", sp_univ_p3)
cat("============================================================\n")
