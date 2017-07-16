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

#############
# ASTER GDEM2
#############

report("Extracting ASTER GDEM2 elevation data")

# Load the ASTER GDEM2 elevation data
# This file has already been mosaiced and clipped to France with a 1 km buffer
aster_dir <- file.path(data_dir, "aster", "gdem2", "france")
aster_dem <- file.path(aster_dir, "astgtm2_france_dem.tif") %>% raster

# Create a velox object from the raster data
report("  Loading data with velox")
vx <- velox(aster_dem)

# Extract the mean elevation of each buffer
report("  Extracting elevation")
elevation <-
  parallel_extract(vx, squares, fun = mean, ncores = ncores) %>%
  cbind("modis_grid_id" = squares$id, "elevation" = .)

# Add the MODIS grid id, save, and clear memory
path <- file.path(extracts_dir, "modis_1km_aster_dem.rds")
paste("  Saving to", path) %>% report
saveRDS(elevation, path)
rm(aster_dir, aster_dem, vx, elevation)

#############
# EU DEM v1.0
#############

report("Extracting Copernicus EU-DEM v1.0 elevation data")

# Load the Copernicus EU-DEM 1.0 elevation data
# This file has already been mosaiced and clipped to France with a 1 km buffer
eu_dem_dir <- file.path(data_dir, "copernicus", "eu-dem", "france")
eu_dem_v10 <- file.path(eu_dem_dir, "eu_dem_v10_france.tif") %>% raster

# Create a velox object from the raster data
report("  Loading data with velox")
vx <- velox(eu_dem_v10)

# Extract the mean elevation of each buffer
report("  Extracting elevation")
elevation <-
  parallel_extract(vx, squares, fun = mean, ncores = ncores) %>%
  cbind("modis_grid_id" = squares$id, "elevation" = .)

# Add the MODIS grid id, save, and clear memory
path <- file.path(extracts_dir, "modis_1km_eu_dem_v10.rds")
paste("  Saving to", path) %>% report
saveRDS(elevation, path)
rm(eu_dem_v10, vx, elevation)

#############
# EU DEM v1.1
#############

report("Extracting Copernicus EU-DEM v1.1 elevation data")

# Load the Copernicus EU-DEM 1.1 elevation data
# This file has already been mosaiced and clipped to France with a 1 km buffer
eu_dem_v11 <- file.path(eu_dem_dir, "eu_dem_v11_france.tif") %>% raster

# Create a velox object from the raster data
report("  Loading data with velox")
vx <- velox(eu_dem_v11)

# Extract the mean elevation of each buffer
report("  Extracting elevation")
elevation <-
  parallel_extract(vx, squares, fun = mean, ncores = ncores) %>%
  cbind("modis_grid_id" = squares$id, "elevation" = .)

# Save the result, clear memory, and reset the reference grid
path <- file.path(extracts_dir, "modis_1km_eu_dem_v11.rds")
paste("  Saving to", path) %>% report
saveRDS(elevation, path)
rm(eu_dem_v11, vx, elevation)

#########
# IGN DEM
#########

report("Extracting IGN elevation data")

# Load the IGN BDALTI elevation data
# This file has already been mosaiced and clipped to France with a 1 km buffer
ign_dir <- file.path(data_dir, "ign", "BDALTI", "france")
ign_dem <- file.path(ign_dir, "BDALTIV2_25M_FXX_MNT_LAMB93_IGN69.tif") %>% raster

# Create a velox object from the raster data
report("  Loading data with velox")
vx <- velox(ign_dem)

# Extract the mean elevation of each buffer
report("  Extracting elevation")
elevation <-
  parallel_extract(vx, squares, fun = mean, ncores = ncores) %>%
  cbind("modis_grid_id" = squares$id, "elevation" = .)

# Save the result, clear memory, and reset the reference grid
path <- file.path(extracts_dir, "modis_1km_ign_dem.rds")
paste("  Saving to", path) %>% report
saveRDS(elevation, path)
rm(ign_dir, ign_dem, vx, elevation)


report("Done")
