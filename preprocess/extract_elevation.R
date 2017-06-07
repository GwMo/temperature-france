# Extract elevation data from various datasets using the reference grid

library(magrittr) # %>% pipe-like operator
library(parallel) # parallel computation
library(sp)       # classes and methods for spatial data
library(raster)   # methods to manipulate gridded spatial data
library(velox)    # c++ accelerated raster manipulation

data_dir <- file.path("~", "data") %>% path.expand
model_dir <- file.path("~", "temperature-france") %>% path.expand
output_dir <- file.path(model_dir, "data")
dir.create(output_dir, showWarnings = FALSE)
setwd(output_dir)

# Load helper functions
file.path(model_dir, "helpers", "report.R") %>% source
file.path(model_dir, "helpers", "parallel_extract.R") %>% source

# Load the reference grid and save its original column names
report("Loading MODIS reference grid")
grid <- file.path(model_dir, "grids", "modis_grid.rds") %>% readRDS
grid_col_names <- names(grid)

# Load the 1 km square buffers
report("Loading 1 km square buffers")
squares <- file.path(model_dir, "buffers", "modis_square_1km.rds") %>% readRDS

# Use 12 cores for parallel extraction
ncores <- 12

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
grid$elevation <- parallel_extract(vx, squares, fun = mean, ncores = ncores)

# Save the result, clear memory, and reset the reference grid
path <- file.path(output_dir, "modis_1km_aster_dem.rds")
paste("  Saving to", path) %>% report
saveRDS(grid@data, path)
rm(aster_dir, aster_dem, vx)
grid <- grid[ , grid_col_names]

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
grid$elevation <- parallel_extract(vx, squares, fun = mean, ncores = ncores)

# Save the result, clear memory, and reset the reference grid
path <- file.path(output_dir, "modis_1km_eu_dem_v10.rds")
paste("  Saving to", path) %>% report
saveRDS(grid@data, path)
rm(eu_dem_v10, vx)
grid <- grid[ , grid_col_names]

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
grid$elevation <- parallel_extract(vx, squares, fun = mean, ncores = ncores)

# Save the result, clear memory, and reset the reference grid
path <- file.path(output_dir, "modis_1km_eu_dem_v11.rds")
paste("  Saving to", path) %>% report
saveRDS(grid@data, path)
rm(eu_dem_dir, eu_dem_v11, vx)
grid <- grid[ , grid_col_names]

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
grid$elevation <- parallel_extract(vx, squares, fun = mean, ncores = ncores)

# Save the result, clear memory, and reset the reference grid
path <- file.path(output_dir, "modis_1km_ign_dem.rds")
paste("  Saving to", path) %>% report
saveRDS(grid@data, path)
rm(ign_dir, ign_dem, vx)
grid <- grid[ , grid_col_names]

report("Done")
