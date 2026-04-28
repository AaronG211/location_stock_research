# ============================================================
# Table I: Summary Statistics Replication
# Pirinsky & Wang (2006) - "Does Corporate Headquarters
# Location Matter for Stock Returns?"
# ============================================================
# Panel A: Firms and MSAs distribution (2012, 2017, 2022)
# Panel B: Industries per MSA distribution (2012, 2017, 2022)
#
# NOTE: This script uses the FILTERED stocks in cleaned_data/stock_price/
# (i.e., stocks that survived the full Stage-2 pipeline, including the
# MSA >=5 firms + >=2 industries filter and the 24-month continuity rule).
# Upstream filters are NOT re-applied here — a firm-month appears in the
# sample if and only if it has a row in its cleaned stock CSV.
# ============================================================

library(data.table)

BASE_DIR     <- normalizePath("..")
MSA_PANEL    <- file.path(BASE_DIR, "cleaned_data/monthly_msa_panel_180mo.csv")
CLEAN_STOCK  <- file.path(BASE_DIR, "cleaned_data/stock_price")

SNAPSHOT_MONTHS <- c("2012-12", "2017-12", "2022-12")
SNAPSHOT_YEARS  <- c(2012, 2017, 2022)

# ============================================================
# STEP 1: Load MSA Panel (wide -> long) for ticker-month -> MSA lookup
# ============================================================
cat("Loading MSA panel...\n")
msa_wide <- fread(MSA_PANEL, header = TRUE)
setnames(msa_wide, names(msa_wide)[1], "month")

msa_long <- melt(
  msa_wide,
  id.vars       = "month",
  variable.name = "permno",
  value.name    = "cbsa"
)
msa_long <- msa_long[!is.na(cbsa)]
msa_long[, cbsa := as.character(as.integer(cbsa))]
msa_long[, permno := as.character(as.integer(as.numeric(as.character(permno))))]

cat(sprintf("MSA panel: %d permno-month pairs with valid MSA\n", nrow(msa_long)))

# ============================================================
# STEP 2: Read filtered stock files -> (month, ticker, industry)
# Only keep rows for the three snapshot months to stay memory-lean.
# ============================================================
cat("\nScanning cleaned (filtered) stock files...\n")
cleaned_files <- list.files(CLEAN_STOCK, pattern = "\\.csv$", full.names = TRUE)
cat(sprintf("Surviving stock files: %d\n", length(cleaned_files)))

if (length(cleaned_files) == 0) {
  stop("cleaned_data/stock_price/ is empty. Run Stage-2 pipeline first.")
}

rows_list <- vector("list", length(cleaned_files))
for (i in seq_along(cleaned_files)) {
  tryCatch({
    d <- fread(cleaned_files[i],
               select      = c("date", "industry", "location_simple"),
               showProgress = FALSE)
    if (nrow(d) == 0) return(NULL)

    d <- d[!is.na(location_simple)]
    if (nrow(d) == 0) return(NULL)

    d[, month := substr(as.character(date), 1, 7)]
    d <- d[month %in% SNAPSHOT_MONTHS]
    if (nrow(d) == 0) return(NULL)

    # Filename convention: TICKER_PERMNO.csv
    permno <- sub("^.*_([0-9]+)\\.csv$", "\\1", basename(cleaned_files[i]))
    d[, permno := permno]

    rows_list[[i]] <- d[, .(month, permno, industry)]
  }, error = function(e) NULL)

  if (i %% 3000 == 0) cat(sprintf("  Processed %d / %d...\n", i, length(cleaned_files)))
}

snap_rows <- rbindlist(rows_list)
cat(sprintf("Collected %d (snapshot-month, permno) rows from filtered sample\n",
            nrow(snap_rows)))

# Join with MSA panel to attach the CBSA code
snap_rows <- merge(snap_rows, msa_long, by = c("month", "permno"), all.x = FALSE)
cat(sprintf("After MSA join: %d rows\n", nrow(snap_rows)))

# ============================================================
# STEP 3: Panel A - Firms and MSAs distribution
# No re-filtering needed: every row already belongs to a valid sample MSA.
# ============================================================
cat("\nComputing Panel A...\n")

panel_a <- rbindlist(lapply(seq_along(SNAPSHOT_MONTHS), function(i) {
  snap_mo <- SNAPSHOT_MONTHS[i]
  yr      <- SNAPSHOT_YEARS[i]

  snap <- snap_rows[month == snap_mo]
  msa_counts <- snap[, .N, by = cbsa]

  data.table(
    Year              = yr,
    `Number of Firms` = sum(msa_counts$N),
    `Number of MSA`   = nrow(msa_counts),
    Mean              = round(mean(msa_counts$N)),
    Median            = as.integer(median(msa_counts$N)),
    Min.              = min(msa_counts$N),
    Max.              = max(msa_counts$N)
  )
}))

# ============================================================
# STEP 4: Panel B - Industries per MSA distribution
# ============================================================
cat("Computing Panel B...\n")

panel_b <- rbindlist(lapply(seq_along(SNAPSHOT_MONTHS), function(i) {
  snap_mo <- SNAPSHOT_MONTHS[i]
  yr      <- SNAPSHOT_YEARS[i]

  snap <- snap_rows[month == snap_mo & !is.na(industry) & industry != "Other"]
  ind_per_msa <- snap[, .(n_ind = uniqueN(industry)), by = cbsa]

  data.table(
    Year                   = yr,
    `Number of Industries` = uniqueN(snap$industry),
    Mean                   = round(mean(ind_per_msa$n_ind)),
    Median                 = as.integer(median(ind_per_msa$n_ind)),
    Min.                   = min(ind_per_msa$n_ind),
    Max.                   = max(ind_per_msa$n_ind)
  )
}))

# ============================================================
# STEP 5: Print Publication-Style Table
# ============================================================
cat("\n\n")
cat(strrep("=", 70), "\n")
cat("                     Table I: Summary Statistics\n")
cat(strrep("=", 70), "\n")
cat("\nCounts are based on the FILTERED research sample\n")
cat("(MSA >=5 firms AND >=2 FF48 industries, 24-month coverage rule;\n")
cat("the residual 'Other' bucket is excluded from Panel B industry counts).\n\n")

# ----- Panel A -----
cat(strrep("-", 70), "\n")
cat("                              Panel A\n")
cat(strrep("-", 70), "\n")
cat("                                     Number of Firms per MSA\n")
cat(sprintf("%-6s  %-15s  %-12s  %6s  %6s  %5s  %5s\n",
            "Year", "No. of Firms", "No. of MSA", "Mean", "Median", "Min.", "Max."))
cat(strrep("-", 70), "\n")
for (i in seq_len(nrow(panel_a))) {
  r <- panel_a[i]
    cat(sprintf("%-6d  %-15d  %-12d  %6d  %6d  %5d  %5d\n",
                as.integer(r$Year),
                as.integer(r$`Number of Firms`),
                as.integer(r$`Number of MSA`),
                as.integer(r$Mean),
                as.integer(r$Median),
                as.integer(r$`Min.`),
                as.integer(r$`Max.`)))
}

# ----- Panel B -----
cat("\n")
cat(strrep("-", 70), "\n")
cat("                              Panel B\n")
cat(strrep("-", 70), "\n")
cat("                          Number of Industries per MSA\n")
cat(sprintf("%-6s  %-22s  %6s  %6s  %5s  %5s\n",
            "Year", "No. of Industries", "Mean", "Median", "Min.", "Max."))
cat(strrep("-", 70), "\n")
for (i in seq_len(nrow(panel_b))) {
  r <- panel_b[i]
    cat(sprintf("%-6d  %-20d  %6d  %6d  %5d  %5d\n",
                as.integer(r$Year),
                as.integer(r$`Number of Industries`),
                as.integer(r$Mean),
                as.integer(r$Median),
                as.integer(r$`Min.`),
                as.integer(r$`Max.`)))
}
cat(strrep("=", 70), "\n")

# ============================================================
# STEP 6: Save Results to CSV
# ============================================================
out_dir <- file.path(BASE_DIR, "script/03_analysis")

fwrite(panel_a, file.path(out_dir, "table1_panel_a.csv"))
fwrite(panel_b, file.path(out_dir, "table1_panel_b.csv"))
cat("\nResults saved to:\n")
cat(sprintf("  %s/table1_panel_a.csv\n", out_dir))
cat(sprintf("  %s/table1_panel_b.csv\n", out_dir))
