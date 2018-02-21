# Format INSEE gridded 200 m population data
#
# * Transform the population data to a raster
# * Disaggregate to 50 m

library(magrittr) # %>% pipe-like operator
library(foreign)  # support for dbf files
library(sp)       # classes and methods for spatial data
library(raster)   # methods to manipulate gridded spatial data

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")

report("")
report("Formatting INSEE population data")
report("---")

# Create a directory to store the rasterized data
processed_dir <- file.path(constants$work_dir, "insee")
dir.create(processed_dir, showWarnings = FALSE)

# Load the INSEE 200 m population data
report("Loading INSEE 200 m gridded population")
insee_pop <-
  file.path(constants$data_dir, "insee", "200m-carreaux-metropole", "car_m.dbf") %>%
  read.dbf(as.is = TRUE)

# Extract the tile center coordinates from the tile ids
# The tile ids include the LAEA coordinates of the tile's bottom left corner so
# adding 100 gives the coordinates of the tile center
report("Extracting tile coordinates")
insee_pop$x <- substr(insee_pop$idINSPIRE, 24, 30) %>% as.numeric %>% + 100
insee_pop$y <- substr(insee_pop$idINSPIRE, 16, 22) %>% as.numeric %>% + 100

# Drop unneeded columns and transform to a raster
report("Transforming to a raster")
report("  Ignore warning about empty grid columns/rows")
insee_pop <- insee_pop[ , c("x", "y", "ind_c")]
names(insee_pop)[names(insee_pop) == "ind_c"] <- "population"
coordinates(insee_pop) <- ~ x + y
proj4string(insee_pop) <- constants$epsg_3035
gridded(insee_pop) <- TRUE
insee_pop <- raster(insee_pop)

# Save the 200 m raster
path <- file.path(processed_dir, "insee_population_200m.tif")
paste("Saving to", path) %>% report
writeRaster(insee_pop, path, overwrite = TRUE)

# Disaggregate the data from 200 m to 50 m and divide the population by 16
report("Disaggregating to 50 m")
insee_pop <- disaggregate(insee_pop, 4) / 16

# Save the disaggregated raster for reference
path <- file.path(processed_dir, "insee_population_50m.tif")
paste("Saving to", path) %>% report
writeRaster(insee_pop, path, overwrite = TRUE)

report("Done")
