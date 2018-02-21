# Create a 1 square km buffer around each point of the MODIS reference grid.
# The buffers are used to extract data for association with the grid points.
#
# * Project the grid to EPSG:3035, an equal-area projection
# * Create square and round buffers of various widths around each grid point
# * Save in EPSG:2154

library(magrittr) # %>% pipe-like operator
library(sp)       # classes and methods for spatial data
library(rgeos)    # wrapper for GEOS to manipulate vector data
library(parallel) # parallel computation

# Set directories and load helper functions
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")
source("helpers/split_into_n.R")

report("")
report("Generating buffers around the MODIS reference grid points")
report("---")

# Detect the number of cores available
ncores <- get_ncores()

# Load the MODIS reference grid and project to EPSG:3035 (LAEA Europe)
# This is an equal area projection, which will ensure buffers have same area throughout France
grid_3035 <-
  file.path(constants$grid_lst_1km_dir, "grid_lst_1km.rds") %>%
  readRDS %>%
  spTransform(constants$epsg_3035)

# Define a function to buffer in parallel
buffer_parallel <- function(splits, width, capStyle, ncores, proj4) {
  # Buffer the points in each group, reproject to EPSG:2154, and collect the
  # results into a single SpatialPolygonsDataFrame
  buffers <- mclapply(splits, function(splt) {
    gBuffer(splt, width = width * 500, capStyle = capStyle, byid = TRUE) %>%
    spTransform(., proj4)
  }, mc.cores = ncores) %>% do.call(rbind, .)

  # Set the plot order and return the resulting buffers
  buffers@plotOrder <- 1:nrow(buffers)
  buffers
}

# Split the grid points into ncores groups for parallel processing
splits <- split_into_n(grid_3035, ncores)
rm(grid_3035)

# Generate 1 km square buffers around each grid point
for (dist in c(1)) {
  paste("Creating", dist, "km square buffers") %>% report
  buffers <- buffer_parallel(splits, dist, "SQUARE", ncores, constants$epsg_2154)
  path <- file.path(constants$grid_lst_1km_dir, paste0("grid_lst_1km_square_", dist, "km.rds"))
  paste("  Saving to", path) %>% report
  saveRDS(buffers, path)
  rm(buffers)
}

# Generate circular buffers with radius of 1, 25, and 50 km around each grid point
for (dist in c(1, 25, 50)) {
  paste("Creating", dist, "km radius buffers") %>% report
  buffers <- buffer_parallel(splits, dist * 2, "ROUND", ncores, constants$epsg_2154)
  path <- file.path(constants$grid_lst_1km_dir, paste0("grid_lst_1km_radius_", dist, "km.rds"))
  paste("  Saving to", path) %>% report
  saveRDS(buffers, path)
  rm(buffers)
}

report("Done")
