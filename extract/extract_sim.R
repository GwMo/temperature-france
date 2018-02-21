# Extract SIM surface model data for the reference grid cells
#
# For each year
# * For each variable of interest
#   - Load the pre-formatted SIM data
#   - Extract by grid points -> "wide" table with 1 data column per day
# * For each month
#   - For each variable of interest
#     + Subset the extracted data for the month from the varible's "wide" table
#     + Add the MODIS grid id
#     + Gather the data into a single column -> "long" table with 1 data column
#   - Combine the "long" tables for all the variables and save

library(magrittr)   # %>% pipe-like operator
library(parallel)   # parallel computation
library(data.table) # fast data manipulation
library(fst)        # fast serialization
library(sp)         # classes and methods for spatial data
library(raster)     # methods to manipulate gridded spatial data

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")

report("")
report("Extracting Meteo France SIM surface model output")
report("---")

# Load the reference grid
report("Loading LST 1 km reference grid")
grid <- file.path(constants$grid_lst_1km_dir, "grid_lst_1km.rds") %>% readRDS

# Detect the number of cores available
ncores <- get_ncores()
threads_fst(ncores)

report("Extracting Meteo France SIM model predictions")

# Use pre-formatted SIM data
# Each file contains data for a single variable for all dates in a year
sim_dir <- file.path(constants$work_dir, "meteo_france", "sim")

# Project the grid to match the SIM data
grid <-
  list.files(sim_dir, pattern = "^SIM2_\\d{4}.+\\.tif$", full.names = TRUE)[1] %>%
  raster %>%
  proj4string %>%
  spTransform(grid, .)

# Extract and save the data
for (year in constants$model_years) {
  paste("Processing", year) %>% report

  # List all SIM data files for the year
  paths <-
    paste0("^SIM2_", year, "-.+\\.tif$") %>%
    list.files(sim_dir, pattern = ., full.names = TRUE)

  # Extract the variable names from the paths
  variables <- basename(paths) %>% sub("SIM2_\\d{4}-(.+)\\.tif", "\\1", .)

  # Extract data for each variable by the reference grid points
  # The SIM data resolution is 8 km compared to 1 km for our reference grid so extracting by points
  # gives a reasonable approximation, and it is much faster than extracting by buffers
  report("  Extracting data")
  extracts <- mclapply(paths, function(path, grid) {
    # Extract the data for all dates for each grid point and convert to a data.table
    sim_data <- brick(path) %>% raster::extract(grid) %>% as.data.table

    # Name the columns by date
    names(sim_data) %>%
      sub("SIM2_(\\d{4}).+\\.(\\d{1-3})", "\\1-\\2", .) %>%
      as.Date("%Y-%j") %>%
      format.Date("%Y-%m-%d") %>%
      setnames(sim_data, .)

    # Add lst_1km_id and return
    sim_data[, "lst_1km_id" := grid$id]
  }, mc.cores = ncores, grid = grid)
  names(extracts) <- variables
  # 75 seconds, 14.8 GB

  # Create a dir to hold the output
  year_dir <- file.path(constants$grid_lst_1km_extracts_dir, year)
  dir.create(year_dir, showWarnings = FALSE)

  # For each month, combine the data for all variables and save to disk
  for (month in 1:12) {
    paste("  Combining data for", month.name[month]) %>% report

    # List all dates in the month
    start_date <- paste0(year, "-%02d-01") %>% sprintf(month) %>% as.Date
    end_date <- seq.Date(start_date, by = "month", length.out = 2)[2] - 1
    month_dates <- seq.Date(from = start_date, to = end_date, by = 1) %>% format.Date("%Y-%m-%d")
    rm(start_date, end_date)

    # List all date - LST 1 km id combinations
    template <- CJ("date" = month_dates, "lst_1km_id" = grid$id)

    # Add the extracted data for the month to the template
    for (v in variables) {
      # Get the data for the variable for the month and melt it from wide to long
      var_data <-
        extracts[[v]] %>%
        .[, c("lst_1km_id", month_dates), with = FALSE] %>%
        melt(id.vars = "lst_1km_id", variable.name = "date", value.name = v, variable.factor = FALSE)

      # Sort by date then lst 1 km id
      var_data %>% setkeyv(cols = c("date", "lst_1km_id"))

      # Confirm the dates and grid ids match the template, then add the data to the template
      stopifnot(
        identical(
          var_data[, key(var_data), with = FALSE],
          template[, key(template), with = FALSE]
        )
      )
      template[, (v) := var_data[, v, with = FALSE]]
      rm(var_data, v)
    }
    # 18 seconds, 1.6 GB

    # Save to disk
    path <-
      paste0("grid_lst_1km_sim_", year, "-%02d.fst") %>%
      sprintf(., month) %>%
      file.path(year_dir, .)
    paste("    Saving to", path) %>% report
    system.time(
      write_fst(template, path, compress = 100)
    )
    # 20 seconds, 130 MB
    # 50 seconds, 110 MB for RDS

    # Cleanup
    rm(template)
  }

  # Cleanup
  rm(extracts)
}

report("Done")

# Alternative: combine entire years - only do this if plenty of memory
#
# # List all date - LST 1 km id combinations
# template <-
#   seq.Date(
#     from = paste0(year, "-01-01") %>% as.Date,
#     to = paste0(year, "-12-31") %>% as.Date,
#     by = 1
#   ) %>%
#   format.Date("%Y-%m-%d") %>%
#   CJ("date" = ., "lst_1km_id" = grid$id)
#
# # Add all extracted data to the template
# report("  Combining extracts")
# for (v in variables) {
#   paste0("    ", v) %>% report
#
#   # Get the extracted data for the variable
#   var_data <-
#     extracts[[v]] %>%
#     melt(id.vars = "lst_1km_id", variable.name = "date", value.name = v, variable.factor = FALSE)
#
#   # Clear the data from the list of extracts to save memory
#   extracts[[v]] <- NULL
#
#   # Sort by date then lst 1 km id
#   var_data %>% setkeyv(cols = c("date", "lst_1km_id"))
#
#   # Confirm the dates and grid ids match the template, then add the data to the template
#   stopifnot(
#     identical(
#       var_data[, key(var_data), with = FALSE],
#       template[, key(template), with = FALSE]
#     )
#   )
#   template[, (v) := var_data[, v, with = FALSE]]
#   rm(var_data, v)
# }
# rm(extracts)
# # 260 seconds, 18.4 GB
#
# path <-
#   paste0("grid_lst_1km_sim_", year, ".fst") %>%
#   sprintf(., year) %>%
#   file.path(constants$grid_lst_1km_extracts_dir, .)
# paste("  Saving to", path) %>% report
# write_fst(template, path, compress = 100)
# # 235 seconds, 1.5 GB
# # 690 seconds, 1.3 GB for RDS
