# Pirinsky-Wang Replication Model 2 (Value-Weighted Industry)
# Specification: R_it = alpha + beta_loc*RLOC_vw + beta_mkt*MKT + beta_ind*RIND_vw

# Set absolute path for data access
stock_dir <- "/Users/shenghangao/Desktop/research/cleaned_data/stock_price"
files <- list.files(stock_dir, pattern = "\\.csv$", full.names = TRUE)


list_p1 <- list(); list_p2 <- list(); list_p3 <- list()

for (i in seq_along(files)) {
  df <- tryCatch(read.csv(files[i], stringsAsFactors=FALSE), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0) next
  
  df$date <- as.Date(df$date)
  
  # Ensure all VW benchmarks exist and are valid
  valid_idx <- complete.cases(df[, c("ret", "location_return", "market_return", "industry_weighted")])
  df_clean <- df[valid_idx, ]
  
  if (nrow(df_clean) < 24) next
  
  p1 <- df_clean[df_clean$date >= as.Date('2012-01-01') & df_clean$date <= as.Date('2015-12-31'), ]
  p2 <- df_clean[df_clean$date >= as.Date('2016-01-01') & df_clean$date <= as.Date('2019-12-31'), ]
  p3 <- df_clean[df_clean$date >= as.Date('2020-01-01') & df_clean$date <= as.Date('2023-12-31'), ]
  
  run_reg <- function(data) {
    if (nrow(data) >= 24) {
      # Use industry_weighted instead of industry_simple
      model <- tryCatch(lm(ret ~ location_return + market_return + industry_weighted, data = data),
                        error = function(e) NULL)
      if (!is.null(model)) {
        coefs <- coef(model)
        if (length(coefs) == 4 && all(!is.na(coefs))) {
          return(data.frame(beta_loc = coefs["location_return"],
                           beta_mkt = coefs["market_return"],
                           beta_ind = coefs["industry_weighted"]))
        }
      }
    }
    return(NULL)
  }
  
  res1 <- run_reg(p1); if(!is.null(res1)) list_p1[[length(list_p1)+1]] <- res1
  res2 <- run_reg(p2); if(!is.null(res2)) list_p2[[length(list_p2)+1]] <- res2
  res3 <- run_reg(p3); if(!is.null(res3)) list_p3[[length(list_p3)+1]] <- res3
}

summarize_results <- function(res_list, period) {
  res <- do.call(rbind, res_list)
  if (is.null(res)) return()
  n <- nrow(res)
  m <- colMeans(res)
  t <- m / (apply(res, 2, sd) / sqrt(n))
  cat(sprintf("%-15s %10.3f %10.3f %10.3f\n", period, m["beta_loc"], m["beta_mkt"], m["beta_ind"]))
  cat(sprintf("%-15s %10.2f %10.2f %10.2f\n", "t-stat", t["beta_loc"], t["beta_mkt"], t["beta_ind"]))
}

cat("\n============================================================\n")
cat("      Results: Model 2 (Value-Weighted Industry Factor)      \n")
cat("============================================================\n")
cat(sprintf("%-15s %10s %10s %10s\n", "Period", "Beta_Loc", "Beta_Mkt", "Beta_Ind"))
cat("------------------------------------------------------------\n")
summarize_results(list_p1, "2012-2015")
summarize_results(list_p2, "2016-2019")
summarize_results(list_p3, "2020-2023")
cat("============================================================\n")
