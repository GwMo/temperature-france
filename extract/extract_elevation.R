# Extract data for the reference grid cells
#
# For each elevation dataset
# * Load the data as a velox object
# * Extract by 1 km square buffers
# * Add LST 1 km id and save

library(magrittr)   # %>% pipe-like operator
library(parallel)   # parallel computation
library(data.table) # fast data manipulation
library(sp)         # classes and methods for spatial data
library(raster)     # methods to manipulate gridded spatial data
library(velox)      # c++ accelerated raster manipulation

# Set directories and load helper functions
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")
source("helpers/stop_if_exists.R")
source("helpers/split_into_n.R")
source("helpers/parallel_velox_extract.R")
source("helpers/save_as_geotiff.R")

report("")
report("Extracting elevation")
report("---")

# Setup -----------------------------------------------------------------------

# Detect the number of cores available
ncores <- get_ncores()

# Load the 1 km square buffers
report("Loading 1 km square buffers")
bufs <- file.path(constants$grid_lst_1km_dir, "grid_lst_1km_square_1km.rds") %>% readRDS

# Save the grid ids and indexes
grid_ids_indexes <- as.data.table(bufs)[, .(lst_1km_id = id, index)]

# Split the buffers into ncores groups, then remove the unsplit buffers to save memory
splits <- split_into_n(bufs, ncores)
rm(bufs)

# Load the 1 km grid as pixels in sinusoidal projection
# We will use this save rasters of the extracted data for visualisation
pixels <- file.path(constants$grid_lst_1km_dir, "grid_lst_1km_sinu_pixels.rds") %>% readRDS

# List the elevation datasets to process
# Each dataset is pre-mosaiced and clipped to France with a 1 km buffer
elevation_dir <- file.path(constants$work_dir, "elevation")
datasets <- list(
  list(
    name = "astgtm2",
    description = "ASTER GDEM2",
    path = file.path(elevation_dir, "astgtm2", "astgtm2_france_dem.tif")
  ),
  list(
    name = "eu_dem_v10",
    description = "EU-DEM v1.0",
    path = file.path(elevation_dir, "eu-dem_v1-0", "eu_dem_v10_france.tif")
  ),
  list(
    name = "eu_dem_v11",
    description = "EU-DEM v1.1",
    path = file.path(elevation_dir, "eu-dem_v1-1", "eu_dem_v11_france.tif")
  ),
  list(
    name = "bdalti",
    description = "IGN BDALTI v2 25m",
    path = file.path(elevation_dir, "bdalti_v2-0", "BDALTIV2_MNT_25M_LAMB93_IGN69_france.tif")
  )
)
rm(elevation_dir)

# Extract ---------------------------------------------------------------------

for (dataset in datasets) {
  paste("Extracting", dataset$description, "elevation") %>% report

  # Stop if the data has already been extracted
  out_path <-
    paste0("grid_lst_1km_", dataset$name, ".rds") %>%
    file.path(constants$grid_lst_1km_extracts_dir, .)
  stop_if_exists(out_path)

  # Load the elevation data as a velox object
  report("  Loading data with velox:")
  paste0("    ", dataset$path)
  elev_vx <- velox(dataset$path)

  # Extract the mean elevation of each buffer, rounded to nearest integer
  report("  Extracting")
  elev_fun <- function(x) { mean(x) %>% round %>% as.integer }
  elevation <- parallel_velox_extract(elev_vx, splits, fun = elev_fun, ncores = ncores)

  # Add the grid id and transform to a data.table
  result <- copy(grid_ids_indexes)
  result[, elevation := pop]

  # Save ------------------------------------------------------------------------
  paste("  Saving in", dirname(out_path)) %>% report

  # GeoTIFF
  tif_path <- sub("\\.rds$", ".tif", out_path)
  paste0("    ", basename(tif_path)) %>% report
  save_as_geotiff(result, pixels, tif_path)
  rm(tif_path)

  # RDS
  result %>% setkey("lst_1km_id")
  paste0("  ", basename(out_path)) %>% report
  saveRDS(result[, !"index"], out_path)
  rm(out_path)

  # Cleanup
  rm(elev_vx, elevation, result)
}

report("Done")
