# Format SIM weather model output data to allow easy extraction

library(magrittr) # %>% pipe-like operator
library(parallel) # parallel computation
library(sp)       # classes and methods for spatial data
library(raster)   # methods to manipulate gridded spatial data

data_dir <- file.path("~", "data") %>% path.expand
model_dir <- file.path("~", "temperature-france") %>% path.expand

# Load helper functions
file.path(model_dir, "helpers", "report.R") %>% source
file.path(model_dir, "helpers", "get_ncores.R") %>% source

# Detect the number of cores available
ncores <- get_ncores()

################################
# METEO FRANCE SIM WEATHER MODEL
################################

report("Formatting Meteo France SIM weather model predictions")

sim_dir <- file.path(data_dir, "meteo_france", "sim")
annual_dir <- file.path(sim_dir, "annual")

metadata <- file.path(sim_dir, "file_format.csv") %>% read.csv(., as.is = TRUE)
data_files <-
  list.files(annual_dir, full.names = TRUE, pattern = "SIM2_\\d{4}.csv$")

# Process the annual SIM model output files
for (path in data_files) {
  paste("Loading", basename(path)) %>% report

  # Look up column classes from the metadata based on the data file header row
  col_classes <-
    read.csv2(path, as.is = TRUE, header = FALSE, nrow = 1) %>%
    match(metadata$colname, .) %>%
    metadata$coltype[.]

  # Read the data - parallelization does not improve performance
  sim_data <- read.csv2(path, colClasses = col_classes, dec = ".")

  report("  Parsing data")

  # Drop unused columns
  sim_data <- sim_data[c(
    "LAMBX",
    "LAMBY",
    "DATE",
    "PRENEI_Q",
    "PRELIQ_Q",
    "T_Q",
    "TINF_H_Q",
    "TSUP_H_Q",
    "FF_Q",
    "Q_Q",
    "HU_Q",
    "RESR_NEIGE_Q"
  )]

  # Rename columns
  names(sim_data) <- c(
    "x",
    "y",
    "date",
    "precip_solid",
    "precip_liquid",
    "temperature_mean",
    "temperature_min",
    "temperature_max",
    "wind_speed",
    "humidity_specific",
    "humidity_relative",
    "snowpack_water_equiv"
  )

  # Perform some conversions
  sim_data$x <- sim_data$x * 100 # hectometres -> metres
  sim_data$y <- sim_data$y * 100 # hectometres -> metres
  sim_data$date <- as.Date(sim_data$date, "%Y%m%d")

  report("  Transforming to SpatialPixelsDataFrame")

  # Transform to a spatial pixels data frame
  # The SIM model uses the NTF Lambert II étendu projection (EPSG:27572)
  coordinates(sim_data) <- ~ x + y
  proj4string(sim_data) <- "+proj=lcc +lat_1=46.8 +lat_0=46.8 +lon_0=0 +k_0=0.99987742 +x_0=600000 +y_0=2200000 +a=6378249.2 +b=6356515 +towgs84=-168,-60,320,0,0,0,0 +pm=paris +units=m +no_defs"
  gridded(sim_data) <- TRUE

  # Identify the columns that contain variables
  variables <- names(sim_data)[!(names(sim_data) %in% c("x", "y", "date"))]

  # List all dates contained in the data
  dates <-
    seq.Date(from = min(sim_data$date), to = max(sim_data$date), by = 1) %>%
    as.character

  # Create a raster stack for each variable containing data for all dates
  report("  Rasterizing")
  for (v in variables) {
    paste("    Stacking dates for", v) %>% report
    stacked <- mclapply(dates, function(d) {
      layer <- sim_data[sim_data$date == d, v] %>% raster
      names(layer) <- d
      layer
    }, mc.cores = ncores) %>% do.call(stack, .)

    # Save the stacked data
    filename <- substr(dates[1], 1, 4) %>% paste0("SIM2_", ., "-", v, ".tif")
    paste("    Saving as", filename) %>% report
    file.path(annual_dir, filename) %>% writeRaster(stacked, .)
  }

  # Clear memory
  rm(stacked, sim_data)
}

report("Done")
