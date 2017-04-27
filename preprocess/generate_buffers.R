# Create buffers around the points of the reference grid

library("magrittr") # %>% pipe-like operator
library("sp")       # classes and methods for spatial data
library("rgeos")    # wrapper for GEOS to manipulate vector data
                    # (GEOS must be present)
library("parallel") # parallel computation

print("Generating buffers around the reference grid points")

# Set working directory
file.path("~", "data", "r") %>% path.expand %>% setwd

# Load the MODIS reference grid
grid <- readRDS("modis_grid.rds")

# Define EPSG:2154, the RGF93 / Lambert-93 projection
# The official Lambert conformal conic projection for metropolitan France
epsg_2154 <- proj4string(grid)

# Define EPSG:3035, the ETRS89 / LAEA Europe projection
# A Europe-wide Lambert azimuthal equal area projection for statistical mapping
epsg_3035 <- "+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"

# Project the grid to EPSG:3035
# Creating the buffers in EPSG:3035 ensures they have the same area
grid_epsg_3035 <- spTransform(grid, epsg_3035)

# Generate and save 1 km and 3 km square buffers around each grid point
distances <- c(1, 3)
mclapply(
  distances,
  function(dist) {
    paste("Creating", dist, "km square buffer") %>% print
    buf <-
      gBuffer(grid_epsg_3035, width = dist * 500, byid = TRUE, capStyle = "SQUARE") %>%
      spTransform(., epsg_2154)
    filename <- paste("modis_grid_square_", dist, "km.rds", sep = "")
    paste("Saving", filename) %>% print
    saveRDS(buf, filename)
  },
  mc.cores = 2
)

# Generate and save 1 km to 15 km circular buffers around each grid point
distances <- c(1, 3, 5, 10, 15)
mclapply(
  distances,
  function(dist) {
    paste("Creating", dist, "km buffer") %>% print
    buf <-
      gBuffer(grid_epsg_3035, width = dist * 500, byid = TRUE) %>%
      spTransform(., epsg_2154)
    filename <- paste("modis_grid_buffer_", dist, "km.rds", sep = "")
    paste("Saving", filename) %>% print
    saveRDS(buf, filename)
  },
  mc.cores = 4
)
