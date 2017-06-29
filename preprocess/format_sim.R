# Format SIM surface model output data to facilitate extraction
#
# For each annual data file
# * Load the data
# * Transform to a SpatialPixelsDataFrame
# * For each variable of interest
#   - Create and save a RasterStack with a separate layer for each day of data

library(magrittr)   # %>% pipe-like operator
library(parallel)   # parallel computation
library(data.table) # fast loading of data files
library(sp)         # classes and methods for spatial data
library(raster)     # methods to manipulate gridded spatial data

data_dir <- file.path("~", "data") %>% path.expand
model_dir <- file.path("~", "temperature-france") %>% path.expand
output_dir <- file.path(model_dir, "data", "sim")
dir.create(output_dir, showWarnings = FALSE)

# Load helper functions
file.path(model_dir, "helpers", "report.R") %>% source
file.path(model_dir, "helpers", "get_ncores.R") %>% source

# Detect the number of cores available
ncores <- get_ncores()

################################
# METEO FRANCE SIM SURFACE MODEL
################################

report("Formatting Meteo France SIM surface model predictions")

sim_dir <- file.path(data_dir, "meteo_france", "sim")

metadata <- file.path(sim_dir, "file_format.csv") %>% read.csv(., as.is = TRUE)
data_files <-
  file.path(sim_dir, "annual") %>%
  list.files(., full.names = TRUE, pattern = "^SIM2_\\d{4}\\.csv$")

# Process the annual SIM model output files
for (path in data_files) {
  # Create a directory to store the formatted data
  annual_output_dir <-
    basename(path) %>%
    gsub("SIM2_(\\d{4})\\.csv", "\\1", .) %>%
    file.path(output_dir, .)
  dir.create(annual_output_dir, showWarnings = FALSE)
  setwd(annual_output_dir)

  paste("Loading", basename(path)) %>% report

  # Look up column classes from the metadata based on the data file header row
  col_classes <-
    fread(path, header = FALSE, nrow = 1) %>%
    match(metadata$colname, .) %>%
    metadata$coltype[.]

  # List the columns we want
  select_cols <- c(
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
  )

  # Read the data - parallelization does not improve performance
  sim_data <- fread(path, colClasses = col_classes, select = select_cols)
  # 10 seconds
  # 319 MB

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
  # 348 MB

  report("  Transforming to SpatialPixelsDataFrame")

  # Transform to a SpatialPointsDataFrame
  coordinates(sim_data) <- ~ x + y
  # 10 seconds
  # 550 MB

  # Set the projection
  # The SIM model uses the NTF Lambert II étendu projection which is based on
  # the deprecated NTF geodetic system. In theory Lambert II étendu corresponds
  # to EPSG:27572, but in fact it differs by up to 5 m. It is essentially the
  # same as ESRI:102582 (differs by < 1 m). The best way to transform is using
  # the IGN-provided NTF to RGF93 shift grid ntf_r93.gsb - for details see
  # https://grasswiki.osgeo.org/wiki/IGNF_register_and_shift_grid_NTF-RGF93
  proj4string(sim_data) <- "+proj=lcc +nadgrids=ntf_r93.gsb,null +lat_1=46.8 +lat_0=46.8 +lon_0=0 +k_0=0.99987742 +x_0=600000 +y_0=2200000 +a=6378249.2 +b=6356515 +towgs84=-168,-60,320,0,0,0,0 +pm=paris +units=m +no_defs"

  # Tranform to a SpatialPixelsDataFrame
  gridded(sim_data) <- TRUE
  # 5 seconds
  # 565 MB

  # Identify the columns that contain variables
  variables <- names(sim_data)[!(names(sim_data) %in% c("x", "y", "date"))]

  # List all dates contained in the data
  dates <-
    seq.Date(from = min(sim_data$date), to = max(sim_data$date), by = 1) %>%
    as.character

  # Create a raster stack for each variable containing data for all dates
  report("  Rasterizing")
  lapply(variables, function(v) {
    paste("    Stacking dates for", v) %>% report

    # Stacking the layers in parallel requires a lot of memory, and if there is
    # not enough some of the threads will be killed resulting in NULL values in
    # the list that is returned, which results in an error when stacking:
    #   Error in stack.default(NULL, NULL, <S4 object of class "RasterLayer">, :
    #     at least one vector element is required
    stacked <- mclapply(dates, function(d) {
      layer <- sim_data[sim_data$date == d, v] %>% raster
      names(layer) <- d
      layer
    }, mc.cores = ncores) %>% do.call(stack, .)
    # 25 seconds

    # Save the stacked data
    output_path <-
      substr(dates[1], 1, 4) %>% paste0("SIM2_", ., "-", v, ".tif") %>%
      file.path(annual_output_dir, .)
      paste("    Saving to", output_path) %>% report
    writeRaster(stacked, output_path, overwrite = TRUE)
    # 5 seconds
  })

  # Clear memory
  rm(sim_data)
}

report("Done")
