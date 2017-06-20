# Create a reference grid covering metropolitan France (except Corsica) aligned
# to the MODIS Aqua LST data. Assumes the check_modis_alignment script reported
# no significant misalignment between modis products.

library(magrittr)  # %>% pipe-like operator
library(sp)        # classes and methods for spatial data
library(rgdal)     # wrapper for GDAL and proj.4 to manipulate spatial data
                   # (GDAL and proj.4 must be present)
library(raster)    # methods to manipulate gridded spatial data
library(gdalUtils) # extends rgdal and raster to manipulate HDF4 files
                   # (GDAL must have been built with HDF4 support)

message("Creating a reference grid from MODIS Aqua LST data")
message("USE CAUTION if check_modis_alignment script reported misaligned MODIS products\n")

# Set directories
data_dir <- file.path("~", "data") %>% path.expand
model_dir <- file.path("~", "temperature-france") %>% path.expand
grid_dir <- file.path(model_dir, "grids") %>% path.expand
dir.create(grid_dir, recursive = TRUE, showWarnings = FALSE)
setwd(grid_dir)

# Load helper functions
file.path(model_dir, "helpers", "report.R") %>% source

# Find the tiles for the first date with MODIS Aqua LST data
tiles <-
  file.path(data_dir, "modis", "aqua", "MYD11A1.006") %>%
  dir(., full.names = TRUE) %>%
  .[1] %>%
  list.files(., full.names = TRUE, pattern = "\\.hdf$")

# Load the night LST dataset for each tile and clear the values
report("Loading MODIS Aqua LST tiles")
rasters <- sapply(tiles, function(tile) {
  get_subdatasets(tile) %>%         # list the scientific datasets for the tile
  .[grepl("LST_Night_1km$", .)] %>% # select the night LST dataset
  raster %>%                        # load the dataset as a raster
  raster                            # clear the values to speed up mosaicing
})
names(rasters) <- NULL # clear the list names to avoid an error when mosaicing

# Load a shapefile of France with 500 m buffers and project to match the tiles
france_2154 <-
  file.path(data_dir, "ign", "borders", "france_epsg-2154_buf500m.shp") %>%
  shapefile

# Project the shapefile to match the tiles
france_sinu <- rasters[[1]] %>% projection %>% spTransform(france_2154, .)

# Mosaic the tiles and clip to France
report("Mosaicing and clipping")
mos <-
  do.call(merge, rasters) %>% # mosaic the rasters - use merge because there is no overlap between the tiles
  crop(., france_sinu) %>%    # crop to France
  brick(., nl = 5)            # create a RasterBrick with 5 layers

# Add layer names
names(mos) <- c("x", "y", "row", "col", "mask")

# Add cell coordinates and row and column numbers
# Ignore any warnings about not being able to read cell values because no file
report("Getting cell latitude and longitude")
report("  Ignore any warnings about not being able to read values because no file")
values(mos$x) <- xFromCell(mos, 1:ncell(mos))
values(mos$y) <- yFromCell(mos, 1:ncell(mos))
values(mos$row) <- rowFromCell(mos, 1:ncell(mos))
values(mos$col) <- colFromCell(mos, 1:ncell(mos))

# Mask by France: cells with center < 500 m outside of France get a value of 1
report("Masking France")
mos$mask <- setValues(mos$mask, 1) %>% mask(., france_sinu)

# Save the mosaic as a GeoTIFF for reference
filename <- "modis_grid_sinusoidal.tif"
paste("Saving mosaic to", filename) %>% report
writeRaster(mos, filename, overwrite = TRUE)

# Convert the mosaic cells in France to a spatial points dataframe
report("Converting to spatial points dataframe")
pts <- mos[mos$mask == 1] %>% .[, c("x", "y", "row", "col")] %>% as.data.frame
coordinates(pts) <- ~ x + y
projection(pts) <- projection(mos)

# Assign an id to each cell and reorder the data columns
pts$id <- 1:nrow(pts)
pts <- pts[, c("id", "row", "col")]

# Project the spatial points dataframe to EPSG:2154
pts <- spTransform(pts, projection(france_2154))

# Save the spatial points dataframe to an rds file
filename <- "modis_grid.rds"
paste("Saving points to", filename) %>% report
saveRDS(pts, filename)
