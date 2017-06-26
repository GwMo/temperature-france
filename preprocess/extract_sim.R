# Extract SIM data using the reference grid

library(magrittr) # %>% pipe-like operator
library(parallel) # parallel computation
library(tidyr)    # data tidying
library(fst)      # fast serialization
library(sp)       # classes and methods for spatial data
library(raster)   # methods to manipulate gridded spatial data

data_dir <- file.path("~", "data") %>% path.expand
model_dir <- file.path("~", "temperature-france") %>% path.expand
output_dir <- file.path(model_dir, "data", "sim")
dir.create(output_dir, showWarnings = FALSE)

# Load helper functions
file.path(model_dir, "helpers", "report.R") %>% source
file.path(model_dir, "helpers", "get_ncores.R") %>% source

# Load the reference grid
report("Loading MODIS reference grid")
grid <- file.path(model_dir, "grids", "modis_grid.rds") %>% readRDS

# Assign the reference grid points to groups of 10,000
groups <- ceiling(1:length(grid) / 10000)

# Detect the number of cores available
ncores <- get_ncores()

################################
# METEO FRANCE SIM WEATHER MODEL
################################

report("Extracting Meteo France SIM weather model predictions")

sim_dir <- file.path(data_dir, "meteo_france", "sim", "annual")

# Parse the variable names from the data file names
variables <-
  list.files(sim_dir, pattern = "^SIM2_\\d{4}-.+\\.tif$") %>%
  gsub("SIM2_\\d{4}-(.+)\\.tif", "\\1", .) %>%
  unique

# Extract and save the data
for (year in as.character(2000:2016)) {
  paste("Processing", year) %>% report

  # Create a directory to hold the extracted data for this year
  year_dir <- file.path(output_dir, year)
  dir.create(year_dir, showWarnings = FALSE)
  setwd(year_dir)

  # Extract the data for each variable
  # Produces a "wide" table for each variable with a column for each date
  report("  Extracting data")
  extracted <- lapply(variables, function(variable) {
    paste0("    ", variable) %>% report

    # Load the data for the variable as a raster brick
    sim_data <-
      paste0("SIM2_", year, "-", variable, ".tif") %>%
      file.path(sim_dir, .) %>%
      brick

    # Extract data from the raster stack by the reference grid points
    # The SIM data resolution is 8 km compared to 1 km for our reference grid
    # so extracting by points gives a reasonable approximation, and it is much
    # faster than disaggregating the SIM data and then extracting by buffers
    mclapply(1:max(groups), function(i) {
      spTransform(grid[groups == i, 1], proj4string(sim_data)) %>%
      extract(sim_data, .)
    }, mc.cores = ncores) %>% do.call(rbind, .) %>% as.data.frame
  })
  names(extracted) <- variables
  # 20 cores
  #   18 seconds / variable = 163 seconds
  #   1.84 GB / variable = 16.5 GB

  # Gather the data for all variables by month and save to disk
  report("  Gathering data")
  lapply(1:12, function(month) {
    paste0("    ", month.name[month]) %>% report

    # For each variable, gather all data for the month into a single column
    # Produces a "long" table for each variable with one column for all dates
    gathered <- lapply(extracted, function(var_data) {
      # Parse the variable from the first column name
      variable <-
        names(var_data)[1] %>%
        gsub("SIM2_\\d{4}\\.(\\w+)\\.\\d+", "\\1", .)

      # Parse the dates from the column names
      dates <-
        names(var_data) %>%
        gsub("SIM2_(\\d{4}).+\\.(\\d+)", "\\1-\\2", .) %>% # Extract year and day of year
        as.Date(., format = "%Y-%j") %>%                   # Convert to date
        format(., "%Y-%m-%d")                              # Format as YYYY-MM-DD

      # Create a vector that selects all dates or column names for the month
      selector <-
        paste0(year, "-%02d") %>%
        sprintf(., month) %>%
        grepl(., dates)

      # Get the data for the month and rename the columns by date
      month_data <- var_data[, selector]
      names(month_data) <- dates[selector]

      # Add the MODIS grid id
      month_data$modis_grid_id <- grid$id

      # Gather the data for all dates into a single column
      gather_(month_data, "date", variable, dates[selector])
    })
    # 20 cores
    #   1.1 seconds / variable = 10 seconds
    #   389 MB / variable = 3.5 GB

    # Get all MODIS grid id - date pairs from the data for the first variable
    key_cols <- gathered[[1]][c("modis_grid_id", "date")]

    # Combine the data into a single table keyed on MODIS grid id and date
    gathered <-
      lapply(gathered, function(var_data) { var_data[3] }) %>%
      do.call(cbind, .) %>%
      cbind(key_cols, .)
    # 1.53 GB

    # Save the data in fst file format with 50% compression
    # write.fst is 35x faster than saveRDS
    filename <-
      paste0("modis_grid_sim_", year, "-%02d.fst") %>%
      sprintf(., month)
    paste0("      Saving as", filename) %>% report
    write.fst(gathered, filename, compress = 50)
  })
  # 20 cores
  #  16 seconds / month = 194 seconds

  # Clear memory
  rm(extracted)
}

report("Done")
