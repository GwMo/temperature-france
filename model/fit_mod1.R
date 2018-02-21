
library(magrittr)
library(parallel)
library(data.table)
library(fst)
library(Matrix) # required by lme4
library(lme4)

# library(sp)
# library(mapview)

source("helpers/constants.R")
source("helpers/abbreviate_colnames.R")
source("helpers/get_ncores.R")
source("helpers/report.R")
source("helpers/rmse.R")
source("helpers/split_for_cv.R")

# Functions -------------------------------------------------------------------

# Calculate descriptive statistics for a model variant or cross-validation
calculate_stats <- function(obj) {
  # Make an empty list to hold the statistics
  stats <- list()

  # R^2, intercept + slope, and RMSE
  # Ta vs Tap
  all_lm <- paste(obj$t_col, "~", obj$pred_col) %>% as.formula %>% lm(data = obj$data)
  stats$r2       <- summary(all_lm)$r.squared
  stats$i        <- summary(all_lm)$coef["(Intercept)", "Estimate"]
  stats$i.se     <- summary(all_lm)$coef["(Intercept)", "Std. Error"]
  stats$slope    <- summary(all_lm)$coef[obj$pred_col, "Estimate"]
  stats$slope.se <- summary(all_lm)$coef[obj$pred_col, "Std. Error"]
  if (!is.null(obj$model)) {
    stats$rmse <- obj$model %>% residuals %>% rmse
  } else {
    stats$rmse <- all_lm %>% residuals %>% rmse
  }

  # Spatial R^2 and RMSE
  # For each station, mean Ta vs mean Tap
  spatial <- obj$data[, .(
                          t_bar    = mean(get(obj$t_col)),
                          pred_bar = mean(get(obj$pred_col))
                         ), by = stn_id]
  spatial_lm <- lm(t_bar ~ pred_bar, data = spatial)
  stats$r2.space <- summary(spatial_lm)$r.squared
  stats$rmse.space <- spatial_lm %>% residuals %>% rmse

  # Temporal R^2
  # For each observation, delta Ta vs delta Tap
  # Delta Ta  = Ta - mean Ta at that station
  # Delta Tap = Tap - mean Tap at that station
  temporal <- obj$data[spatial, .(
                                  t = get(obj$t_col),
                                  t_bar,
                                  pred = get(obj$pred_col),
                                  pred_bar
                                 ), on = "stn_id"]
  temporal[, delta_t := t - t_bar]
  temporal[, delta_pred := pred - pred_bar]
  temporal_lm <- lm(delta_t ~ delta_pred, data = temporal)
  stats$r2.time <- summary(temporal_lm)$r.squared
  stats$rmse.time <- temporal_lm %>% residuals %>% rmse

  # Return the statistics
  stats
}

# Cross-validate a model variant and calculate cv statistics
cross_validate <- function(m1, folds = 10, ncores) {
  # Get info needed for cross validation from the model variant
  # This is more memory-efficient than passing the entire model variant to the cv loop
  cv <- list(
    data = m1$data,
    folds = folds,
    formula = formula(m1$model),
    pred_col = paste0(m1$pred_col, "_cv"),
    t_col = m1$t_col
  )

  # Perfom k-fold cross-validation
  cv$data <- mclapply(1:folds, function(fold, cv) {
    # Split the data into train and test sets
    sets <- split_for_cv(cv$data, cv$folds)

    # Fit the formula to the training set
    model <- lmer(cv$formula, data = sets$trainset)

    # Predict temperature for the test set
    preds <- predict(model, newdata = sets$testset, allow.new.levels = TRUE, re.form = NULL)
    sets$testset[, (cv$pred_col) := preds]
    sets$testset[, fold := fold]

    # Return the test set, including predictions
    sets$testset
  }, mc.cores = ncores, cv = cv) %>% rbindlist

  # Calculate cross-validation statistics
  cv$stats <- calculate_stats(cv)

  # Return the cross-validation
  cv
}

# Fit a model variant and calculate initial statistics
fit_m1 <- function(m1) {
  # Scale the predictor data
  m1$data[, (m1$predictors) := lapply(.SD, scale), .SDcols = m1$predictors]

  # Fit the model
  m1$model <- m1_formula(m1$t_col, m1$predictors) %>% lmer(data = m1$data)

  # Predict temperature
  m1$pred_col <- "pred_m1"
  m1$data[, (m1$pred_col) := predict(m1$model)]

  # Calculate initial statistics
  m1$stats <- calculate_stats(m1)

  # Return the variant
  m1
}

# Initialize a model variant - a list of the info needed to fit a model (formula, data, etc.)
init_variant <- function(predictors, iter, mod1) {
  # Copy the iteration parameters (t_col, lst_col, etc.) and add the predictors
  m1 <- iter
  m1$predictors <- predictors

  # Subset the required data and drop any rows with missing data
  m1$data <-
    c("date", "stn_id", "lat", "lon", "stn_type", "stn_elev", "lst_1km_id", "climate_type",
      m1$t_col, m1$predictors) %>%
    mod1[, ., with = FALSE] %>%
    na.omit

  # Return the variant
  m1
}

# Load and format the mod1 data for a model
load_mod1 <- function(iter) {
  # Convert period to a regex pattern
  if (is.null(iter$period) || iter$period == "all years") {
    year <- "\\d{4}"
  } else if (iter$period %in% constants$model_years) {
    year <- iter$period
  } else {
    stop(paste("Unrecognized time period:", iter$period))
  }

  # Load the data
  pattern <- paste0("grid_lst_1km_mod1_", iter$sat_pass, "_", year, "-\\d{2}.rds")
  mod1 <-
    constants$grid_lst_1km_joined_dir %>%
    list.files(full.names = TRUE, pattern = pattern, recursive = TRUE) %>%
    lapply(readRDS) %>%
    rbindlist

  # Abbreviate column names and format columns
  report("  Formatting mod1 data")
  mod1 %>% abbreviate_colnames %>% setnames(mod1, .)
  mod1[, date := as.Date(date)]
  mod1[, lst_degc := get(iter$lst_col) - 273.15] # K -> deg C

  # Return the data
  mod1
}

# Construct an m1 formula from a temperature column and set of predictors
# Do *not* pass large objects (e.g. a model variant) to this function - the formula will include
# this function's environment, so it will use as much memory as the passed objects
m1_formula <- function(t_col, predictors) {
  paste(predictors, collapse = " + ") %>%
    paste(t_col, "~", ., "+ (1 + lst_degc | date/climate_type)") %>%
    as.formula
}

# Identify columns that vary per iteration (e.g. aqua_day_lst vs aqua_night_lst)
set_iter_cols <- function(iter) {
  sat <- strsplit(iter$sat_pass, "_")[[1]][1]
  iter$lst_col <- paste0(iter$sat_pass, "_lst")
  iter$emis_col <- paste0(sat, "_emis")
  iter$ndvi_col <- paste0(sat, "_ndvi")
  iter$sim_col <- paste0("sim_", iter$t_col)
  iter
}

# # Save a list of model variants along with a summary
# save_variants <- function(variants) {
#   # Construct a filename prefix e.g. tmin~aqua_night_lst_m1_YYYY-mm-dd_HHMMSS
#   run <-
#     Sys.time() %>%
#     format("_%Y-%m-%d_%H%M%S") %>%
#     paste0(variants[[1]]$t_col, "~", variants[[1]]$lst_col, "_", variants[[1]]$model_level, .)
#
#   # Save a summary of the variants
#   smry <- summarize_variants(variants)
#   smry_path <- paste0(run, "_summary.csv") %>% file.path(constants$grid_lst_1km_results_dir, .)
#   paste0("    ", basename(smry_path)) %>% report
#   write.csv(smry, file = smry_path)
#
#   # For each variant, save identifying info, the model, and the cv data
#   for (m1 in variants) {
#     m1[c("period", "model_level", "t_col", "lst_col", "model")]
#   }
#   models_path <- paste0(run, "_models.rds") %>% file.path(constants$grid_lst_1km_results_dir, .)
#   paste0("    ", basename(models_path)) %>% report
#   saveRDS(models, models_path)
#   # 60 seconds, 303 MB
# }

# Return a single-column data frame summarizing a model variant
summarize <- function(m1, report_tval_for = NULL) {
  # Identify the variant
  smry <- m1[c("period", "model_level", "t_col", "lst_col")]

  # Add model statistics
  stats <- c("r2", "r2.space", "r2.time", "rmse", "rmse.space", "rmse.time")
  smry[stats] <- m1$stats[stats] %>% sprintf("%.4f", .)

  # Add cross-validation statistics if they exist
  if (!is.null(m1$cv)) {
    stats <- c(stats, "i", "i.se", "slope", "slope.se")
    smry[paste0("cv.", stats)] <- m1$cv$stats[stats] %>% sprintf("%.4f", .)
  }

  # Add t value of model predictors
  # If a vector of t values to report was specified, report those t values (even if NA). This will
  # allow us to combine summaries of different model variants in a single data frame for printing.
  tvals <- coef(summary(m1$model))[, "t value"]
  if (is.null(report_tval_for)) {
    report_tval_for <- names(tvals)
  }
  smry[paste0(report_tval_for, ".t")] <- tvals[report_tval_for] %>% sprintf("%.2f", .)

  # Return as a single-column data frame with named rows
  smry %>% as.data.frame %>% transpose %>% set_rownames(names(smry))
}

# Return a data frame with a summary column for each model variant
summarize_variants <- function(variants) {
  # Summarize all model variants, including t value for all predictors (even unused ones)
  report_tval_for = c("(Intercept)", all_predictors)
  lapply(variants, summarize, report_tval_for = report_tval_for) %>%
    do.call(cbind, .) %>%
    set_colnames(1:length(variants))
}

# Run -------------------------------------------------------------------------

ncores <- get_ncores()

# The combinations to model e.g. tmax ~ aqua_day_lst
iterations <- list(
  list(period = 2008, t_col = "tmin",  sat_pass = "aqua_night"),
  list(period = 2008, t_col = "tmin",  sat_pass = "terra_night"),
  list(period = 2008, t_col = "tmin",  sat_pass = "aqua_day"),
  list(period = 2008, t_col = "tmin",  sat_pass = "terra_day"),
  list(period = 2008, t_col = "tmean", sat_pass = "aqua_day"),
  list(period = 2008, t_col = "tmean", sat_pass = "terra_day"),
  list(period = 2008, t_col = "tmean", sat_pass = "aqua_night"),
  list(period = 2008, t_col = "tmean", sat_pass = "terra_night"),
  list(period = 2008, t_col = "tmax",  sat_pass = "aqua_day"),
  list(period = 2008, t_col = "tmax",  sat_pass = "terra_day"),
  list(period = 2008, t_col = "tmax",  sat_pass = "aqua_night"),
  list(period = 2008, t_col = "tmax",  sat_pass = "terra_night")

  # list(period = 2003, t_col = "tmin",  sat_pass = "aqua_night"),
  # list(period = 2003, t_col = "tmin",  sat_pass = "terra_night"),
  # list(period = 2003, t_col = "tmin",  sat_pass = "aqua_day"),
  # list(period = 2003, t_col = "tmin",  sat_pass = "terra_day"),
  # list(period = 2003, t_col = "tmean", sat_pass = "aqua_day"),
  # list(period = 2003, t_col = "tmean", sat_pass = "terra_day"),
  # list(period = 2003, t_col = "tmean", sat_pass = "aqua_night"),
  # list(period = 2003, t_col = "tmean", sat_pass = "terra_night"),
  # list(period = 2003, t_col = "tmax",  sat_pass = "aqua_day"),
  # list(period = 2003, t_col = "tmax",  sat_pass = "terra_day"),
  # list(period = 2003, t_col = "tmax",  sat_pass = "aqua_night"),
  # list(period = 2003, t_col = "tmax",  sat_pass = "terra_night"),
  #
  # list(period = 2012, t_col = "tmin",  sat_pass = "aqua_night"),
  # list(period = 2012, t_col = "tmin",  sat_pass = "terra_night"),
  # list(period = 2012, t_col = "tmin",  sat_pass = "aqua_day"),
  # list(period = 2012, t_col = "tmin",  sat_pass = "terra_day"),
  # list(period = 2012, t_col = "tmean", sat_pass = "aqua_day"),
  # list(period = 2012, t_col = "tmean", sat_pass = "terra_day"),
  # list(period = 2012, t_col = "tmean", sat_pass = "aqua_night"),
  # list(period = 2012, t_col = "tmean", sat_pass = "terra_night"),
  # list(period = 2012, t_col = "tmax",  sat_pass = "aqua_day"),
  # list(period = 2012, t_col = "tmax",  sat_pass = "terra_day"),
  # list(period = 2012, t_col = "tmax",  sat_pass = "aqua_night"),
  # list(period = 2012, t_col = "tmax",  sat_pass = "terra_night"),
)

for (iter in iterations) {
  # Identify columns that vary per iteration (e.g. aqua_day_lst vs aqua_night_lst) and specify
  # that this is a m1 model (cell-days with Ta and Ts)
  iter <- set_iter_cols(iter)
  iter$model_level <- "m1"
  paste(iter$period, ":", iter$model_level, ":", iter$t_col, "~", iter$lst_col) %>% report

  # Load and format mod1 data
  paste("  Loading", iter$sat_pass, "mod1 data for", iter$period) %>% report
  mod1 <- load_mod1(iter)
  # str(mod1)

  # All the predictors (used for summarizing variants)
  all_predictors <- c("lst_degc", iter$sim_col, iter$emis_col, iter$ndvi_col, "elev", "pop",
    "clc_urban_built", "clc_urban_green", "clc_mine_dump", "clc_field", "clc_shrub", "clc_forest",
    "clc_bare", "clc_water")

  # Sets of predictors to test
  predictor_sets <- list(
    c("lst_degc", iter$sim_col, iter$emis_col, iter$ndvi_col, "elev", "pop"),
    c("lst_degc",               iter$emis_col, iter$ndvi_col, "elev", "pop"),
    c("lst_degc", iter$sim_col,                iter$ndvi_col, "elev", "pop"),
    c("lst_degc", iter$sim_col, iter$emis_col,                "elev", "pop"),
    c("lst_degc", iter$sim_col,                               "elev", "pop"),
    c("lst_degc", iter$sim_col, iter$emis_col, iter$ndvi_col, "elev", "pop", "clc_urban_built", "clc_urban_green", "clc_mine_dump", "clc_field", "clc_shrub", "clc_forest", "clc_bare", "clc_wetland", "clc_water"),
    c("lst_degc",               iter$emis_col, iter$ndvi_col, "elev", "pop", "clc_urban_built", "clc_urban_green", "clc_mine_dump", "clc_field", "clc_shrub", "clc_forest", "clc_bare", "clc_wetland", "clc_water"),
    c("lst_degc", iter$sim_col,                iter$ndvi_col, "elev", "pop", "clc_urban_built", "clc_urban_green", "clc_mine_dump", "clc_field", "clc_shrub", "clc_forest", "clc_bare", "clc_wetland", "clc_water"),
    c("lst_degc", iter$sim_col, iter$emis_col,                "elev", "pop", "clc_urban_built", "clc_urban_green", "clc_mine_dump", "clc_field", "clc_shrub", "clc_forest", "clc_bare", "clc_wetland", "clc_water"),
    c("lst_degc", iter$sim_col,                               "elev", "pop", "clc_urban_built", "clc_urban_green", "clc_mine_dump", "clc_field", "clc_shrub", "clc_forest", "clc_bare", "clc_wetland", "clc_water")
    # c("lst_degc", iter$sim_col, iter$emis_col, iter$ndvi_col, "elev", "pop",  # 1
    #   "clc_urban_built", "clc_urban_green",                  "clc_field", "clc_shrub", "clc_forest", "clc_bare",                "clc_water"),
    # c("lst_degc",                                             "elev", "pop",  # 2
    #   "clc_urban_built", "clc_urban_green",                  "clc_field", "clc_shrub", "clc_forest", "clc_bare",                "clc_water"),
    # c("lst_degc", iter$sim_col, iter$emis_col, iter$ndvi_col, "elev", "pop",  # 3
    #   "clc_urban_built", "clc_urban_green",                  "clc_field", "clc_shrub", "clc_forest", "clc_bare"),
    # c("lst_degc",                                             "elev", "pop",  # 4
    #   "clc_urban_built", "clc_urban_green",                  "clc_field", "clc_shrub", "clc_forest", "clc_bare"),
    # c("lst_degc", iter$sim_col, iter$emis_col, iter$ndvi_col, "elev", "pop",  # 5
    #   "clc_urban_built",                                     "clc_field", "clc_shrub", "clc_forest", "clc_bare"),
    # c("lst_degc",                                             "elev", "pop",  # 6
    #   "clc_urban_built",                                     "clc_field", "clc_shrub", "clc_forest", "clc_bare")
  )

  # Initialize a model variant for each set of predictors
  # A model variant is a list of all info needed to fit a model (formula, data, etc.)
  paste("  Initializing", length(predictor_sets), "model variants") %>% report
  variants <- lapply(predictor_sets, init_variant, iter = iter, mod1 = mod1)

  # Fit the variants
  # ~ 100 seconds 2008
  # ~ 600 seconds all years
  paste("  Fitting", length(variants), "model variants") %>% report
  variants <- mclapply(variants, fit_m1, mc.cores = ncores)
  report("  Done fitting")

  # Inspect the initial statistics
  smry <- summarize_variants(variants)
  print(smry)

  # Save the inital statistics
  run <-
    Sys.time() %>%
    format("_%Y-%m-%d_%H%M%S") %>%
    paste0(iter$period, "_", iter$t_col, "~", iter$lst_col, "_", iter$model_level, .)
  smry_path <- paste0(run, "_summary.csv") %>% file.path(constants$grid_lst_1km_results_dir, .)
  paste("    Saving", basename(smry_path)) %>% report
  write.csv(smry, file = smry_path)
  rm(run, smry_path, smry)

  # Clear memory
  rm(all_predictors, iter, mod1, predictor_sets, variants)

  # # Cross-validate the variants (cross-validate function is parallelized)
  # #  50 seconds,  40 MB for 2008
  # # 860 seconds, 590 MB for all years
  # paste("  Cross-validating", length(variants), "model variants") %>% report
  # for (i in 1:length(variants)) {
  #   paste0("    ", i) %>% report
  #   variants[[i]]$cv <- cross_validate(variants[[i]], folds = 10, ncores = ncores)
  #   rm(i)
  # }
  # report("  Done cross-validating")
  #
  # # Inspect the full statistics
  # summarize_variants(variants) %>% print
  #
  # # Save results
  # report("  Saving results")
  # save_variants(variants)
}





# mod1[, error_m1 := temperature_min - pred_m1]
#
# map_points <- function(mod1) {
#   coordinates(mod1) <- ~ longitude + latitude
#   proj4string(mod1) <- CRS(constants$epsg_4326)
#   mapview(mod1)
# }
# station_errors <-
#   mod1[, .(insee_id, latitude, longitude, error = abs(mean(error_m1))), by = "insee_id"]
# map_points(station_errors[error > 3, ])


# 2008 (15 minutes)
# tmin ~ aqua_night_lst
# ---------------------
# model_level                   m1             m1             m1             m1             m1             m1             m1             m1             m1             m1
# t_col                       tmin           tmin           tmin           tmin           tmin           tmin           tmin           tmin           tmin           tmin
# lst_col           aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
# r2                        0.9432         0.9432         0.9432         0.9432         0.9434         0.9434         0.9434         0.9434         0.9434         0.9434
# r2.space                  0.8523         0.8523         0.8519         0.8519         0.8538         0.8543         0.8544         0.8543         0.8543         0.8538
# r2.time                   0.9579         0.9579         0.9579         0.9579         0.9579         0.9578         0.9578         0.9578         0.9578         0.9579
# rmse                      1.5113         1.5113         1.5107         1.5107         1.5078         1.5083         1.5083         1.5084         1.5084         1.5078
# rmse.space                0.8908         0.8907         0.8918         0.8917         0.8858         0.8846         0.8845         0.8846         0.8846         0.8858
# rmse.time                 1.2208         1.2208         1.2204         1.2205         1.2209         1.2215         1.2215         1.2214         1.2214         1.2209
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
# cv.r2                      0.941         0.9409         0.9409         0.9412         0.9412          0.941         0.9417         0.9415         0.9413         0.9417
# cv.r2.space               0.8509         0.8504          0.853         0.8501         0.8517           0.85         0.8524          0.854         0.8539         0.8522
# cv.r2.time                0.9561         0.9563         0.9563         0.9562         0.9563         0.9562         0.9565         0.9562         0.9562         0.9566
# cv.rmse                    1.539         1.5426         1.5422         1.5384         1.5374         1.5399         1.5317         1.5346         1.5351         1.5332
# cv.rmse.space             0.9122         0.9171          0.916         0.9127         0.9127         0.9159         0.9046         0.9037         0.9038         0.9096
# cv.rmse.time              1.2427         1.2407         1.2399         1.2407          1.239         1.2415         1.2388          1.242         1.2398         1.2372
# cv.i                      0.0053          0.001          7e-04          0.001         0.0087         0.0016         0.0049         0.0068        -0.0012         0.0051
# cv.i.se                   0.0035         0.0035         0.0035         0.0035         0.0035         0.0035         0.0035         0.0035         0.0035         0.0035
# cv.slope                  0.9996         0.9997         1.0007         1.0004         0.9993         0.9998         0.9998         0.9997         1.0006         0.9998
# cv.slope.se                4e-04          4e-04          4e-04          4e-04          4e-04          4e-04          4e-04          4e-04          4e-04          4e-04
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
# (Intercept).t             137.63         137.64         137.19          137.2         138.45         138.74         138.98         138.94         138.94         138.45
# lst_degc.t                 35.42          35.42          35.25          35.25          34.91          35.12           35.2          35.19          35.19          34.91
# sim_tmin.t                 417.5         417.18         417.04         416.65         418.93         419.35         431.42          431.5          431.5         418.93
# aqua_emis.t                 <NA>           1.51           <NA>            2.2           1.83            1.8           1.77           <NA>           <NA>           1.83
# aqua_ndvi.t                 <NA>           <NA>          -6.59          -6.78          -0.67           <NA>           <NA>           <NA>           <NA>          -0.67
# elev.t                    -13.05         -13.13         -13.44         -13.57          -0.93          -0.92           <NA>           <NA>           <NA>          -0.93
# pop.t                      30.35          28.48          27.82          26.69          25.85          26.15          26.14          26.64          26.64          25.85
# clc_urban_built.t           <NA>           <NA>           <NA>           <NA>         -29.06          -29.4         -29.49         -29.53          -18.2         -17.61
# clc_urban_green.t           <NA>           <NA>           <NA>           <NA>         -30.13         -30.48          -30.6          -30.7         -21.34         -20.63
# clc_mine_dump.t             <NA>           <NA>           <NA>           <NA>         -16.16         -16.37         -16.41         -16.42          -7.74          -7.45
# clc_field.t                 <NA>           <NA>           <NA>           <NA>         -30.09         -30.49         -30.62         -30.57         -19.17          -18.7
# clc_shrub.t                 <NA>           <NA>           <NA>           <NA>         -29.46         -29.87         -29.99         -29.95         -18.65         -18.17
# clc_forest.t                <NA>           <NA>           <NA>           <NA>         -31.76         -32.32          -32.6         -32.57         -21.77            -21
# clc_bare.t                  <NA>           <NA>           <NA>           <NA>         -31.95         -32.19         -33.38         -33.33         -23.21         -21.99
# clc_wetland.t               <NA>           <NA>           <NA>           <NA>          -9.99         -10.05         -10.06          -9.99           <NA>           <NA>
# clc_water.t                 <NA>           <NA>           <NA>           <NA>           <NA>           <NA>           <NA>           <NA>           9.99           9.99

# cv.r2.space               0.8518         0.8531         0.8484         0.8493         0.8496         0.8507         0.8543         0.8543         0.8536         0.8543
# cv.r2.time                0.9565         0.9563         0.9564         0.9563         0.9565         0.9563         0.9565         0.9565         0.9562         0.9562
# cv.rmse                   1.5383         1.5411         1.5424         1.5411         1.5334         1.5365         1.5332         1.5323         1.5364         1.5357
# cv.rmse.space             0.9152         0.9138         0.9205         0.9105         0.9078          0.909         0.9067         0.9025         0.9067         0.9041
# cv.rmse.time              1.2368         1.2401         1.2401         1.2409         1.2378         1.2394         1.2361         1.2386         1.2415         1.2408
# cv.i                       5e-04          8e-04         0.0011          4e-04          0.002         0.0023         0.0072        -0.0011        -0.0041        -0.0034
# cv.i.se                   0.0035         0.0035         0.0035         0.0035         0.0035         0.0035         0.0035         0.0035         0.0035         0.0035
# cv.slope                  1.0005         1.0006         1.0003         1.0001         0.9992         0.9999         0.9993         1.0007         1.0001         1.0004
# cv.slope.se                4e-04          4e-04          4e-04          4e-04          4e-04          4e-04          4e-04          4e-04          4e-04          4e-04




































# model_level                   m1             m1             m1             m1             m1             m1
# t_col                       tmin           tmin           tmin           tmin           tmin           tmin
# lst_col           aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst
# -----------------------------------------------------------------------------------------------------------
# r2                        0.9434         0.9434         0.9434         0.9434         0.9434         0.9434
# r2.space                  0.8538         0.8543         0.8544         0.8543         0.8538         0.8543
# r2.time                   0.9579         0.9578         0.9578         0.9578         0.9579         0.9578
# rmse                      1.5078         1.5083         1.5083         1.5084         1.5078         1.5084
# rmse.space                0.8858         0.8846         0.8845         0.8846         0.8858         0.8846
# rmse.time                 1.2209         1.2215         1.2215         1.2214         1.2209         1.2214
# -----------------------------------------------------------------------------------------------------------
# cv.r2                     0.9412          0.941         0.9417         0.9415         0.9417         0.9413
# cv.r2.space               0.8517           0.85         0.8524          0.854         0.8522         0.8539
# cv.r2.time                0.9563         0.9562         0.9565         0.9562         0.9566         0.9562
# cv.rmse                   1.5374         1.5399         1.5317         1.5346         1.5332         1.5351
# cv.rmse.space             0.9127         0.9159         0.9046         0.9037         0.9096         0.9038
# cv.rmse.time               1.239         1.2415         1.2388          1.242         1.2372         1.2398
# cv.i                      0.0087         0.0016         0.0049         0.0068         0.0051        -0.0012
# cv.i.se                   0.0035         0.0035         0.0035         0.0035         0.0035         0.0035
# cv.slope                  0.9993         0.9998         0.9998         0.9997         0.9998         1.0006
# cv.slope.se                4e-04          4e-04          4e-04          4e-04          4e-04          4e-04
# -----------------------------------------------------------------------------------------------------------
# (Intercept).t             138.45         138.74         138.98         138.94         138.45         138.94
# lst_degc.t                 34.91          35.12           35.2          35.19          34.91          35.19
# sim_tmin.t                418.93         419.35         431.42          431.5         418.93          431.5
# aqua_emis.t                 1.83            1.8           1.77           <NA>           1.83           <NA>
# aqua_ndvi.t                -0.67           <NA>           <NA>           <NA>          -0.67           <NA>
# elev.t                     -0.93          -0.92           <NA>           <NA>          -0.93           <NA>
# pop.t                      25.85          26.15          26.14          26.64          25.85          26.64
# clc_urban_built.t         -29.06          -29.4         -29.49         -29.53         -17.61          -18.2
# clc_urban_green.t         -30.13         -30.48          -30.6          -30.7         -20.63         -21.34
# clc_mine_dump.t           -16.16         -16.37         -16.41         -16.42          -7.45          -7.74
# clc_field.t               -30.09         -30.49         -30.62         -30.57          -18.7         -19.17
# clc_shrub.t               -29.46         -29.87         -29.99         -29.95         -18.17         -18.65
# clc_forest.t              -31.76         -32.32          -32.6         -32.57            -21         -21.77
# clc_bare.t                -31.95         -32.19         -33.38         -33.33         -21.99         -23.21
# clc_wetland.t              -9.99         -10.05         -10.06          -9.99           <NA>           <NA>
# clc_water.t                 <NA>           <NA>           <NA>           <NA>           9.99           9.99





# All years (14.5 hours)
# tmin ~ aqua_night_lst
# ---------------------
# model_level                   m1             m1             m1             m1             m1             m1            m1             m1             m1             m1
# t_col                       tmin           tmin           tmin           tmin           tmin           tmin          tmin           tmin           tmin           tmin
# lst_col           aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lstaqua_night_lst aqua_night_lst aqua_night_lst aqua_night_lst
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
# r2                        0.9475         0.9475         0.9475         0.9475         0.9477         0.9477        0.9477         0.9477         0.9477         0.9477
# r2.space                  0.8545         0.8545         0.8542         0.8543         0.8557         0.8562        0.8559         0.8559         0.8559         0.8557
# r2.time                   0.9604         0.9604         0.9604         0.9604         0.9603         0.9603        0.9604         0.9604         0.9604         0.9603
# rmse                      1.5521         1.5521         1.5516         1.5516         1.5491         1.5495        1.5496         1.5496         1.5496         1.5491
# rmse.space                0.9119         0.9117         0.9117         0.9114          0.907         0.9067        0.9074         0.9076         0.9076          0.907
# rmse.time                 1.2784         1.2785         1.2783         1.2784         1.2789         1.2792        1.2788         1.2788         1.2788         1.2789
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
# cv.r2                     0.9456         0.9456         0.9457         0.9455         0.9457         0.9458        0.9458         0.9458         0.9458         0.9458
# cv.r2.space               0.8503         0.8506         0.8495         0.8493         0.8503         0.8523          0.85         0.8519         0.8521         0.8517
# cv.r2.time                 0.959         0.9589          0.959         0.9589         0.9588         0.9589        0.9589         0.9589         0.9589         0.9589
# cv.rmse                   1.5794         1.5794         1.5784         1.5796          1.577         1.5764        1.5762         1.5764         1.5773         1.5764
# cv.rmse.space             0.9283         0.9271         0.9283         0.9303         0.9258         0.9218        0.9248         0.9237         0.9231         0.9236
# cv.rmse.time              1.3012         1.3009         1.3002         1.3014         1.3022         1.3019        1.3015         1.3013         1.3013         1.3015
# cv.i                       0.004         0.0018         0.0012         0.0053         0.0023         0.0037        0.0044          0.002         0.0025         0.0045
# cv.i.se                    0.001          0.001          0.001          0.001          0.001          0.001         0.001          0.001          0.001          0.001
# cv.slope                  0.9996         1.0001         1.0001         0.9997         0.9998         0.9997        0.9997         0.9999         0.9998         0.9999
# cv.slope.se                1e-04          1e-04          1e-04          1e-04          1e-04          1e-04         1e-04          1e-04          1e-04          1e-04
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
# (Intercept).t             532.93         532.95         531.51         531.51         534.02         535.14        528.39         528.22         528.22         534.01
# lst_degc.t                149.77         149.79         149.65          149.7         148.58         148.92         147.9         147.87         147.87         148.58
# sim_tmin.t               1527.68        1526.02         1526.5        1524.51        1530.46        1531.59       1564.23        1564.84        1564.84        1530.46
# aqua_emis.t                 <NA>          11.02           <NA>          14.48          10.12          10.04          10.5           <NA>           <NA>          10.12
# aqua_ndvi.t                 <NA>           <NA>         -28.86         -30.41          -4.56           <NA>          <NA>           <NA>           <NA>          -4.56
# elev.t                     -8.17           -8.7          -9.25         -10.05          29.22          29.59          <NA>           <NA>           <NA>          29.22
# pop.t                      91.91          88.76          81.98          81.62          79.24          80.46         80.83          80.99          80.99          79.24
# clc_urban_built.t           <NA>           <NA>           <NA>           <NA>         -91.28          -92.9        -91.61         -91.78         -62.98         -61.91
# clc_urban_green.t           <NA>           <NA>           <NA>           <NA>         -95.76         -97.32        -95.85         -96.33         -72.99          -71.8
# clc_mine_dump.t             <NA>           <NA>           <NA>           <NA>         -42.14         -43.11        -42.28         -42.27         -18.73          -18.4
# clc_field.t                 <NA>           <NA>           <NA>           <NA>         -93.99         -95.88        -94.26          -93.9         -64.77         -64.83
# clc_shrub.t                 <NA>           <NA>           <NA>           <NA>         -92.04         -93.95         -92.3         -91.95         -63.03         -63.09
# clc_forest.t                <NA>           <NA>           <NA>           <NA>        -102.56        -105.12        -102.7        -102.48          -75.6         -75.78
# clc_bare.t                  <NA>           <NA>           <NA>           <NA>        -100.34        -101.88        -97.69         -97.39          -71.1         -74.83
# clc_wetland.t               <NA>           <NA>           <NA>           <NA>         -27.57         -28.12        -27.93         -27.44           <NA>           <NA>
# clc_water.t                 <NA>           <NA>           <NA>           <NA>           <NA>           <NA>          <NA>           <NA>          27.44          27.57
