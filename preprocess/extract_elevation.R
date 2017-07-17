# Extract elevation data for the reference grid cells
#
# For each elevation dataset
# * Load the data as a raster
# * Convert to a velox object
# * Extract by 1 km square buffers
# * Add the MODIS grid id and save

library(magrittr) # %>% pipe-like operator
library(parallel) # parallel computation
library(sp)       # classes and methods for spatial data
library(raster)   # methods to manipulate gridded spatial data
library(velox)    # c++ accelerated raster manipulation

# Set directories and load helper functions
file.path("~", "temperature-france", "helpers", "set_dirs.R") %>% source
file.path(helpers_dir, "report.R") %>% source
file.path(helpers_dir, "get_ncores.R") %>% source
file.path(helpers_dir, "parallel_extract.R") %>% source


report("Extracting elevation data")


# Load the 1 km square buffers
report("Loading 1 km square buffers")
squares <- file.path(buffers_dir, "modis_square_1km.rds") %>% readRDS

# Detect the number of cores available
ncores <- get_ncores()

# List the elevation datasets to process
# Each dataset is pre-mosaiced and clipped to France with a 1 km buffer
datasets <- list(
  aster_dem = list(
    description = "ASTER GDEM2",
    data_dir = file.path(data_dir, "aster", "gdem2", "france"),
    filename = "astgtm2_france_dem.tif"
  ),
  eu_dem_v10 = list(
    description = "EU-DEM v1.0",
    data_dir = file.path(data_dir, "copernicus", "eu-dem", "france"),
    filename = "eu_dem_v10_france.tif"
  ),
  eu_dem_v11 = list(
    description = "EU-DEM v1.1",
    data_dir = file.path(data_dir, "copernicus", "eu-dem", "france"),
    filename = "eu_dem_v11_france.tif"
  ),
  aster_dem = list(
    description = "IGN BDALTI v2 25m",
    data_dir = file.path(data_dir, "ign", "BDALTI", "france"),
    filename = "BDALTIV2_25M_FXX_MNT_LAMB93_IGN69.tif"
  )
)

# Process each elevation dataset
for (name in names(datasets)) {
  dataset <- datasets[[name]]
  paste("Extracting", dataset$description, "elevation data") %>% report

  # Load the elevation data as a velox object
  report("  Loading data with velox")
  vx <- file.path(dataset$data_dir, dataset$filename) %>% raster %>% velox

  # Extract the mean elevation of each buffer and round to nearest integer
  report("  Extracting elevation")
  elev <- parallel_extract(vx, squares, fun = mean, ncores = ncores) %>% round

  # Add the MODIS grid id, transform to a data frame, and save
  path <- paste0("modis_1km_", name, ".rds") %>% file.path(extracts_dir, .)
  paste("  Saving to", path) %>% report
  data.frame(
    "modis_grid_id" = squares$id,
    "elevation" = elev
  ) %>% saveRDS(., path)
}


report("Done")
