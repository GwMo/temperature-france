# Format Joly (2010) climate types data
#
# * Assign the Lambert II étendu projection
# * Adjust coordinates
# * Disaggregate to 50 m

library(magrittr) # %>% pipe-like operator
library(sp)       # classes and methods for spatial data
library(raster)   # methods to manipulate gridded spatial data

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")

report("")
report("Formatting Joly (2010) climate types data")
report("---")

# Create a directory to store the rasterized data
processed_dir <- file.path(constants$work_dir, "climate_types")
dir.create(processed_dir, showWarnings = FALSE)

# Load the data
data_path <-
  file.path(constants$data_dir, "joly_climats_france", "Typologie des climats français.asc")
report("Loading data")
paste0("  ", data_path) %>% report
climate_types <- raster(data_path)
rm(data_path)

# Set the projection
projection(climate_types) <- constants$lambert_etendu

# Adjust the coordinates
# The data are clearly offset to the north of their proper location. Upon close inspection, they
# seem to be offset just over 1000000 m north and slightly west. I settled on a correction of
# 1000000 m south, plus 250 m (one pixel) east and west. This seems to produce good alignment with
# the borders of France, and could be explained if the data were erroneously saved using the false
# northing of the Lambert III projection (rather than Lambert II). The one pixel offset might be
# explained if somehow the coordinates of the top left corner of each pixel became associated with
# the bottom right corner.
report("Shifting data 250 m east and 1000250 m south")
climate_types <- shift(climate_types, x = 250, y = -1000250)

# Save the adjusted data as a GeoTIFF
path <- file.path(processed_dir, "joly_2010_climate_types.tif")
paste("Saving as", path) %>%
writeRaster(climate_types, path, overwrite = TRUE)
rm(path)

# Disaggregate to 50 m
report("Disaggregating to 50 m")
climate_types <- disaggregate(climate_types, 5)

# Save the disaggregated data
path <- file.path(processed_dir, "joly_2010_climate_types_50m.tif")
paste("Saving as", path)
writeRaster(climate_types, path, overwrite = TRUE)

report("Done")
