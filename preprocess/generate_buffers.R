# Generate buffers around the points of the reference grid

library(magrittr) # %>% pipe-like operator
library(sp)       # classes and methods for spatial data
library(rgeos)    # wrapper for GEOS to manipulate vector data
                  # (GEOS must be present)
library(parallel) # parallel computation

# Function to display messages with a timestamp
display <- function(msg) {
  paste(Sys.time(), msg, sep = ": ") %>% message
}

display("Generating buffers around the reference grid points")

# Set directories
base_dir <- file.path("~", "temperature-france") %>% path.expand
buffer_dir <- file.path(base_dir, "buffers")
dir.create(buffer_dir, showWarnings = FALSE)
setwd(buffer_dir)

# Load the MODIS reference grid
grid <- file.path(base_dir, "grids", "modis_grid.rds") %>% readRDS

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

# Define a function to buffer in parallel using 24 cores
ncores <- 24
buffer_parallel <- function(pts, width, capStyle = "ROUND") {
  # Assign the grid points to ncores groups
  groups <- ceiling(1:nrow(pts) / (nrow(pts) / ncores))

  # Buffer the points in each group, reproject to EPSG:2154, and collect the
  # results into a single SpatialPolygonsDataFrame
  buffers <- mclapply(1:ncores, function(group) {
    pts[groups == group, ] %>%
    gBuffer(., width = width * 500, capStyle = capStyle, byid = TRUE) %>%
    spTransform(., epsg_2154)
  }, mc.cores = ncores) %>%
  do.call(rbind, .)

  # Set the plot order and return the resulting buffers
  buffers@plotOrder <- 1:nrow(buffers)
  buffers
}

# Generate and 1 km and 3 km square buffers around each grid point
for (distance in c(1, 3)) {
  paste("Creating", distance, "km square buffer") %>% display
  buffers <- buffer_parallel(grid_3035, distance, capStyle = "SQUARE")
  filename <- paste0("modis_square_", distance, "km.rds")
  paste("  Saving", filename) %>% display
  saveRDS(buffers, filename)
  rm(buffers)
}

# Generate and save 1 km to 15 km circular buffers around each grid point
for (distance in c(1, 3, 5, 10, 15)) {
  paste("Creating", distance, "km buffer") %>% display
  buffers <- buffer_parallel(grid_3035, distance)
  filename <- paste0("modis_round_", distance, "km.rds")
  paste("  Saving to", filename) %>% display
  saveRDS(buffers, filename)
  rm(buffers)
}
