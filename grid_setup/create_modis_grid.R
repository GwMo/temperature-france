# Create a reference grid for metropolitan France (except Corsica) aligned to the MODIS LST data
# Be sure to first run check_modis_alignment.R to confirm all MODIS products are aligned
#
# * Mosaic all MODIS Aqua LST tiles for a single date
# * Crop to extent of France
# * Set cell x coordinate, y coordinate, row number, and col number
# * Convert cell centroids to SpatialPoints
# * Remove points that are not in the study area
# * Add latitude / longitude and LST 1 km id
# * Save in EPSG:2154
# * Also save as shapefile and as SpatialPixels in MODIS sinusoidal

library(magrittr)  # %>% pipe-like operator
library(parallel)  # parallel computation
library(sp)        # classes and methods for spatial data
library(rgdal)     # wrapper for GDAL and proj.4 to manipulate spatial data
library(raster)    # methods to manipulate gridded spatial data
library(gdalUtils) # extends rgdal and raster to manipulate HDF4 files

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")
source("helpers/split_into_n.R")

report("")
report("Creating a 1 km reference grid from MODIS Aqua LST data")
report("---")

# Find the tiles for the first date with MODIS Aqua LST data
tiles <-
  file.path(constants$data_dir, "modis", "aqua", "MYD11A1.006") %>%
  dir(., full.names = TRUE) %>%
  .[1] %>%
  list.files(., full.names = TRUE, pattern = "\\.hdf$")

# Load the night LST dataset for each tile and clear the values
report("Loading MODIS Aqua LST tiles")
rasters <- lapply(tiles, function(tile) {
  get_subdatasets(tile) %>%         # list the scientific datasets for the tile
  .[grepl("LST_Night_1km$", .)] %>% # select the night LST dataset
  raster %>%                        # load the dataset as a raster
  raster                            # clear the values to speed up mosaicing
})
rm(tiles)

# Load a shapefile of France and project to match the tiles
france_2154 <-
  file.path(constants$work_dir, "borders", "france_epsg-2154.shp") %>%
  shapefile
france_sinu <- rasters[[1]] %>% projection %>% spTransform(france_2154, .)

# Mosaic the tiles and clip to France
report("Mosaicing and clipping")
mos <-
  do.call(merge, rasters) %>%        # mosaic the rasters - use merge because there is no overlap between the tiles
  crop(., france_sinu, snap = "out") # crop to France
rm(rasters)

# Store the cell coordinates and row and column numbers
# Ignore any warnings about not being able to read cell values because no file
report("Getting cell coordinates")
report("  Ignore any warning about not being able to read values because no file")
mos$x <- xFromCell(mos, 1:ncell(mos))
mos$y <- yFromCell(mos, 1:ncell(mos))
mos$row <- rowFromCell(mos, 1:ncell(mos))
mos$col <- colFromCell(mos, 1:ncell(mos))

# DISABLED b/c masking fails for some cells in the Etang de Berre (and is slow)
# # Mask cells whose centroid is not on in France
# mos$mask <- 1
# mos$mask <- mask(mos_zoom$mask, france_zoom)
# # 410 seconds

# Convert the cell centroids to a spatial points dataframe
report("Converting to spatial points dataframe")
pts <- as.data.frame(mos)
coordinates(pts) <- ~ x + y
projection(pts) <- projection(mos)
rm(mos)

# Store row and col as integers to save space (raster uses numeric by default)
pts$row <- as.integer(pts$row)
pts$col <- as.integer(pts$col)

# Remove points that are not on land in France
# This is more accurate (and faster) than masking the raster before converting to points
report("Removing points outside of France")

# Determine whether points are inside France
ncores <- get_ncores()
in_france <-
  split_into_n(pts, ncores) %>%
  mclapply(function(splt, france) {
    over(splt, france)[, 1]
  }, mc.cores = ncores, france = france_sinu) %>%
  unlist
pts <- pts[!is.na(in_france), ]
rm(in_france, ncores)
# 45 seconds

# Reset the coordinates and data rownames to start at 1
rownames(pts@coords) <- 1:nrow(pts)
rownames(pts@data) <- 1:nrow(pts)

# Ensure the row and col numbers start at 1
# (the first row and column do not contain any cell centroids)
pts$row <- pts$row + (1L - min(pts$row))
pts$col <- pts$col + (1L - min(pts$col))

# Project the points to EPSG:4326 (WGS84) and store the cell latitudes and longitudes
report("Storing latitude and longitude")
pts <- spTransform(pts, constants$epsg_4326)
lng_lat <- coordinates(pts) %>% as.data.frame
pts$latitude <- lng_lat$y
pts$longitude <- lng_lat$x
rm(lng_lat)

# Assign each cell an id based on its latitude and longitude
# Round to 6 decimal places, which implies accuracy of ~ 0.1 m
report("Setting id based on latitude and longitude rounded to 6 decimal places")
pts$id <- paste(sprintf("%.6f", pts$latitude), sprintf("%.6f", pts$longitude), sep = "-")

# Reorder the data columns
pts <- pts[, c("id", "row", "col", "latitude", "longitude")]

# Assign each cell a numeric index
# This is useful for saving rasters, which cannot contain character values
pts$index <- 1:nrow(pts)

# Project the points to EPSG:2154 (Lambert-93)
report("Projecting to EPSG:2154 Lambert-93")
pts <- spTransform(pts, constants$epsg_2154)

# Save the spatial points dataframe to an rds file
path <- file.path(constants$grid_lst_1km_dir, "grid_lst_1km.rds")
paste("Saving to", path) %>% report
saveRDS(pts, path)

# Save as a csv for reference
# This is useful for inspection and converting between id and index
path <- file.path(constants$grid_lst_1km_dir, "grid_lst_1km.csv")
paste("Saving as CSV to", path) %>% report
pts %>% as.data.frame %>% write.csv(path, row.names = FALSE)

# Project to MODIS sinusoidal and save as a shapefile for reference
path <- file.path(constants$grid_lst_1km_dir, "grid_lst_1km_sinu.shp")
paste("Saving as sinusoidal shapefile to", path) %>% report
pts <- spTransform(pts, constants$proj_sinu)
shapefile(pts, path, overwrite = TRUE)

# Drop all but the index, convert to SpatialPixelsDataFrame, and save
# This will facilitate creating rasters of extracted data for visualization
path <- file.path(constants$grid_lst_1km_dir, "grid_lst_1km_sinu_pixels.rds")
paste("Saving as sinusoidal pixels to", path) %>% report
pixels <- pts[, "index"]
gridded(pixels) <- TRUE
saveRDS(pixels, path)

report("Done")
