# Pirinsky-Wang Replication Model 2 (Value-Weighted Industry and Location)
# Specification: R_it = alpha + beta_loc*RLOC_vw + beta_mkt*MKT + beta_ind*RIND_vw
#
# NOTE: All sample filters (MSA >=5 firms, MSA >=2 industries, NA removal,
# minimum data coverage) are now applied upstream in 02_cleaning_processing/.

BASE_DIR  <- normalizePath("..")
stock_dir <- file.path(BASE_DIR, "cleaned_data/stock_price")
files <- list.files(stock_dir, pattern = "\\.csv$", full.names = TRUE)

# Load SP500 annual constituents to construct Ex-ante universes for filtering
sp500 <- read.csv(file.path(BASE_DIR, "raw_data/sp500_annual_constituents_2007_2022.csv"), stringsAsFactors=FALSE)
sp_univ_p1 <- sp500$permno[sp500$year == 2007]  # Universe for 2008-2012
sp_univ_p2 <- sp500$permno[sp500$year == 2012]  # Universe for 2013-2017
sp_univ_p3 <- sp500$permno[sp500$year == 2017]  # Universe for 2018-2022

list_p1 <- list(); list_p2 <- list(); list_p3 <- list()

for (i in seq_along(files)) {
  df <- tryCatch(read.csv(files[i], stringsAsFactors=FALSE), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0) next

  df$date <- as.Date(df$date)

  p1 <- df[df$date >= as.Date('2008-01-01') & df$date <= as.Date('2012-12-31'), ]
  p2 <- df[df$date >= as.Date('2013-01-01') & df$date <= as.Date('2017-12-31'), ]
  p3 <- df[df$date >= as.Date('2018-01-01') & df$date <= as.Date('2022-12-31'), ]
  
  run_reg <- function(data) {
    data <- data[complete.cases(data[, c(
      "ret",
      "location_weighted",
      "market_return",
      "industry_weighted"
    )]), ]
    if (nrow(data) >= 24) {
      model <- tryCatch(
        lm(
          ret ~ location_weighted + market_return + industry_weighted,
          data = data
        ),
        error = function(e) NULL
      )
      if (!is.null(model)) {
        coefs <- coef(model)
        if (length(coefs) == 4 && all(!is.na(coefs))) {
          return(data.frame(permno = data$permno[1],
                           beta_loc = coefs["location_weighted"],
                           beta_mkt = coefs["market_return"],
                           beta_ind = coefs["industry_weighted"]))
        }
      }
    }
    return(NULL)
  }
  
  if (!is.null(p1)) { res1 <- run_reg(p1); if(!is.null(res1)) list_p1[[length(list_p1)+1]] <- res1 }
  if (!is.null(p2)) { res2 <- run_reg(p2); if(!is.null(res2)) list_p2[[length(list_p2)+1]] <- res2 }
  if (!is.null(p3)) { res3 <- run_reg(p3); if(!is.null(res3)) list_p3[[length(list_p3)+1]] <- res3 }
}

summarize_results <- function(res_list, period, sp_filter = NULL) {
  res <- do.call(rbind, res_list)
  if (is.null(res)) return()
  
  if (!is.null(sp_filter)) {
    res <- res[res$permno %in% sp_filter, ]
  }
  
  if (nrow(res) == 0) return()
  
  n <- nrow(res)
  target_cols <- c("beta_loc", "beta_mkt", "beta_ind")
  m <- colMeans(res[, target_cols])
  t <- m / (apply(res[, target_cols], 2, sd) / sqrt(n))
  
  cat(sprintf("%-15s %10.3f %10.3f %10.3f\n", period, m["beta_loc"], m["beta_mkt"], m["beta_ind"]))
  cat(sprintf("%-15s %10.2f %10.2f %10.2f\n", paste0("t-stat (N=", n, ")"), t["beta_loc"], t["beta_mkt"], t["beta_ind"]))
}

cat("\n============================================================\n")
cat("  Results: Model 2 (Value-Weighted Industry and Location Factors) - ALL STOCKS \n")
cat("============================================================\n")
cat(sprintf("%-15s %10s %10s %10s\n", "Period", "Beta_Loc", "Beta_Mkt", "Beta_Ind"))
cat("------------------------------------------------------------\n")
summarize_results(list_p1, "2008-2012")
summarize_results(list_p2, "2013-2017")
summarize_results(list_p3, "2018-2022")
cat("============================================================\n")

cat("\n============================================================\n")
cat("   Results: Model 2 (Value-Weighted Industry and Location Factors) - SPY ONLY  \n")
cat("============================================================\n")
cat(sprintf("%-15s %10s %10s %10s\n", "Period", "Beta_Loc", "Beta_Mkt", "Beta_Ind"))
cat("------------------------------------------------------------\n")
summarize_results(list_p1, "2008-2012", sp_univ_p1)
summarize_results(list_p2, "2013-2017", sp_univ_p2)
summarize_results(list_p3, "2018-2022", sp_univ_p3)
cat("============================================================\n")
