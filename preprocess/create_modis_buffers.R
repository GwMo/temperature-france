# Create a 1 square km buffer around each point of the MODIS reference grid.
# The buffers will be used to extract data for association with the grid points.
#
# * Project the MODIS reference grid to EPSG:3035, an equal-area projection
# * Create square and round buffers of varying width around each grid point
# * Project the buffers to EPSG:2154
# * Save

library(magrittr) # %>% pipe-like operator
library(sp)       # classes and methods for spatial data
library(rgeos)    # wrapper for GEOS to manipulate vector data
                  # (GEOS must be present)
library(parallel) # parallel computation

# Set directories and load helper functions
file.path("~", "temperature-france", "helpers", "set_dirs.R") %>% source
file.path(helpers_dir, "report.R") %>% source
file.path(helpers_dir, "get_ncores.R") %>% source


report("Generating buffers around the MODIS reference grid points")


# Load the MODIS reference grid
grid <- file.path(grids_dir, "modis_grid.rds") %>% readRDS

# Define EPSG:2154, the RGF93 / Lambert-93 projection
# The official Lambert conformal conic projection for metropolitan France
epsg_2154 <- proj4string(grid)

# Define EPSG:3035, the ETRS89 / LAEA Europe projection
# A Europe-wide Lambert azimuthal equal area projection for statistical mapping
epsg_3035 <- "+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs"

# Project the grid to EPSG:3035 and clear memory
# Creating the buffers in EPSG:3035 ensures they have the same area
grid_3035 <- spTransform(grid, epsg_3035)
rm(grid)

# Define a function to buffer in parallel
buffer_parallel <- function(pts, width, capStyle = "ROUND") {
  # Assign the grid points to ncores groups
  groups <- ceiling(1:nrow(pts) / 5000)

  # Buffer the points in each group, reproject to EPSG:2154, and collect the
  # results into a single SpatialPolygonsDataFrame
  buffers <- mclapply(1:max(groups), function(group) {
    pts[groups == group, ] %>%
    gBuffer(., width = width * 500, capStyle = capStyle, byid = TRUE) %>%
    spTransform(., epsg_2154)
  }, mc.cores = ncores) %>% do.call(rbind, .)

  # Set the plot order and return the resulting buffers
  buffers@plotOrder <- 1:nrow(buffers)
  buffers
}

# Detect the number of cores available
ncores <- get_ncores()

# Generate 1 km and 3 km square buffers around each grid point
for (distance in c(1, 3)) {
  paste("Creating", distance, "km square buffer") %>% report
  buffers <- buffer_parallel(grid_3035, distance, capStyle = "SQUARE")
  path <- file.path(buffers_dir, paste0("modis_square_", distance, "km.rds"))
  paste("  Saving to", path) %>% report
  saveRDS(buffers, path)
  rm(buffers)
}

# Generate 1 km to 15 km circular buffers around each grid point
for (distance in c(1, 3, 5, 10, 15)) {
  paste("Creating", distance, "km buffer") %>% report
  buffers <- buffer_parallel(grid_3035, distance)
  path <- file.path(buffers_dir, paste0("modis_round_", distance, "km.rds"))
  paste("  Saving to", path) %>% report
  saveRDS(buffers, path)
  rm(buffers)
}

report("Done")
