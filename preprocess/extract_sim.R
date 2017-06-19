# Extract SIM data using the reference grid

library(magrittr) # %>% pipe-like operator
library(parallel) # parallel computation
library(tidyr)    # data tidying
library(sp)       # classes and methods for spatial data
library(raster)   # methods to manipulate gridded spatial data

data_dir <- file.path("~", "data") %>% path.expand
model_dir <- file.path("~", "temperature-france") %>% path.expand
output_dir <- file.path(model_dir, "data", "sim")
dir.create(output_dir, showWarnings = FALSE)
setwd(output_dir)

# Load helper functions
file.path(model_dir, "helpers", "report.R") %>% source

# Load the reference grid
report("Loading MODIS reference grid")
grid <- file.path(model_dir, "grids", "modis_grid.rds") %>% readRDS

# Assign the reference grid points to groups of 10,000
groups <- ceiling(1:length(grid) / 10000)

# Use 16 cores for parallel tasks
ncores <- 16

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

years <- as.character(2000:2016)

for (variable in variables) {
  paste("Processing", variable) %>% report

  # Extract the data for all years
  all_data <- lapply(years, function(year) {
    paste("  Extracting data for", year) %>% report

    # Load the data for the year as a raster brick
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
  names(all_data) <- years

  # Format and save the annual extracts in parallel
  mclapply(years, function(year) {
    annual_data <- all_data[[year]]

    # Name the columns of the extracted data by date
    dates <-
      colnames(annual_data) %>%
      gsub("SIM2_(\\d{4}).+\\.(\\d+)", "\\1-\\2", .) %>% # Extract year and day of year
      as.Date(., format = "%Y-%j") %>%                   # Convert to date
      format(., "%Y-%m-%d")                              # Format as YYYY-MM-DD
    names(annual_data) <- dates

    # Add the reference grid row and column numbers
    annual_data$row <- grid$row
    annual_data$col <- grid$col

    # Gather all date columns into a single column
    col_name <- paste("sim", variable, sep = "_")
    paste("  Gathering", year, "data as", col_name) %>% report
    annual_data <- gather_(annual_data, "date", col_name, dates)

    # Save the results
    filename <- paste0("modis_grid_sim_", variable, "-", year, ".rds")
    paste("  Saving", filename) %>% report
    file.path(output_dir, filename) %>% saveRDS(annual_data, ., compress = TRUE)

    # Return NULL to save memory
    NULL
  }, mc.cores = ncores)

  # Clear memory
  rm(all_data)
}

report("Done")
