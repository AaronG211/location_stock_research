# Base R Beta Time Series Regression Script
# Author: Antigravity

# Set absolute path for data access
stock_dir <- "/Users/shenghangao/Desktop/research/cleaned_data/stock_price"
files <- list.files(stock_dir, pattern = "\\.csv$", full.names = TRUE)

cat("============================================================\n")
cat(sprintf("Initializing time-series regressions for %d surviving stocks...\n", length(files)))
cat("============================================================\n")

list_p1 <- list()
list_p2 <- list()
list_p3 <- list()

for (i in seq_along(files)) {
  f <- files[i]
  df <- tryCatch(read.csv(f, stringsAsFactors=FALSE), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0) next
  
  df$date <- as.Date(df$date)
  
  # Remove completely NA columns/rows for the regressions
  valid_idx <- complete.cases(df[, c("ret", "location_return", "market_return", "industry_simple")])
  df_clean <- df[valid_idx, ]
  
  if (nrow(df_clean) < 24) next
  
  # Cut into 3 periods
  p1 <- df_clean[df_clean$date >= as.Date('2012-01-01') & df_clean$date <= as.Date('2015-12-31'), ]
  p2 <- df_clean[df_clean$date >= as.Date('2016-01-01') & df_clean$date <= as.Date('2019-12-31'), ]
  p3 <- df_clean[df_clean$date >= as.Date('2020-01-01') & df_clean$date <= as.Date('2023-12-31'), ]
  
  run_reg <- function(data) {
    if (nrow(data) >= 24) {
      # Use tryCatch to avoid stopping if collinearity causes failure
      model <- tryCatch(lm(ret ~ location_return + market_return + industry_simple, data = data),
                        error = function(e) NULL)
      if (!is.null(model)) {
        coefs <- coef(model)
        # Check if all four coefficients (Intercept + 3 variables) are validly fitted
        if (length(coefs) == 4 && all(!is.na(coefs))) {
          return(data.frame(
            beta_loc = coefs["location_return"],
            beta_mkt = coefs["market_return"],
            beta_ind = coefs["industry_simple"]
          ))
        }
      }
    }
    return(NULL)
  }
  
  res1 <- run_reg(p1)
  if (!is.null(res1)) list_p1[[length(list_p1) + 1]] <- res1
  
  res2 <- run_reg(p2)
  if (!is.null(res2)) list_p2[[length(list_p2) + 1]] <- res2
  
  res3 <- run_reg(p3)
  if (!is.null(res3)) list_p3[[length(list_p3) + 1]] <- res3
}

# Aggregate all individual time-series betas
results_p1 <- do.call(rbind, list_p1)
results_p2 <- do.call(rbind, list_p2)
results_p3 <- do.call(rbind, list_p3)

summarize_betas <- function(res, period_name) {
  if (is.null(res) || nrow(res) == 0) {
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
  cat(sprintf("%-15s %10.2f %10.2f %10.2f\n", "t-stat", t_loc, t_mkt, t_ind))
}

cat("\n============================================================\n")
cat("          Results of Cross-Sectional Average Betas          \n")
cat("============================================================\n")
cat(sprintf("%-15s %10s %10s %10s\n", "Period", "Beta_Loc", "Beta_Mkt", "Beta_Ind"))
cat("------------------------------------------------------------\n")
summarize_betas(results_p1, "2012-2015")
summarize_betas(results_p2, "2016-2019")
summarize_betas(results_p3, "2020-2023")
cat("============================================================\n")
