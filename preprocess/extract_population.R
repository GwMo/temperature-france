# Extract population data for the reference grid cells
#
# * Transform the population data to a raster
# * Disaggregate to 50 m
# * Convert to a velox object
# * Extract by 1 km square buffers
# * Add the MODIS grid id and save

library(magrittr) # %>% pipe-like operator
library(parallel) # parallel computation
library(sp)       # classes and methods for spatial data
library(foreign)  # support for dbf files
library(raster)   # methods to manipulate gridded spatial data
library(velox)    # c++ accelerated raster manipulation

# Set directories and load helper functions
file.path("~", "temperature-france", "helpers", "set_dirs.R") %>% source
file.path(helpers_dir, "report.R") %>% source
file.path(helpers_dir, "get_ncores.R") %>% source
file.path(helpers_dir, "parallel_extract.R") %>% source


report("Extracting INSEE population")


# Load the 1 km square buffers
report("Loading 1 km square buffers")
squares <- file.path(buffers_dir, "modis_square_1km.rds") %>% readRDS

# Detect the number of cores available
ncores <- get_ncores()

##################
# INSEE POPULATION
##################

# Load the INSEE 200 m population data
report("Loading INSEE 200 m population data")
insee_dir <- file.path(data_dir, "insee", "200m-carreaux-metropole")
insee_pop <- file.path(insee_dir, "car_m.dbf") %>% read.dbf(., as.is = TRUE)

# Extract the tile center coordinates from the tile ids
# The tile ids include the LAEA coordinates of the tile's bottom left corner so
# adding 100 gives the coordinates of the tile center
report("Extracting tile coordinates")
insee_pop$x <- substr(insee_pop$idINSPIRE, 24, 30) %>% as.numeric %>% + 100
insee_pop$y <- substr(insee_pop$idINSPIRE, 16, 22) %>% as.numeric %>% + 100

# Drop unneeded columns and load as a raster
report("Loading data as a raster")
report("  Ignore warning about empty grid columns/rows")
insee_pop <- insee_pop[ , c("x", "y", "ind_c")]
names(insee_pop)[3] <- "population"
coordinates(insee_pop) <- ~ x + y
proj4string(insee_pop) <- "+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs"
gridded(insee_pop) <- TRUE
insee_pop <- raster(insee_pop)

# Save the raster for reference
path <- file.path(insee_dir, "insee_population_200m.tif")
paste("Saving raster to", path) %>% report
writeRaster(insee_pop, path, overwrite = TRUE)

# Disaggregate the data from 200 m to 50 m and divide the population by 16
report("Disaggregating to 50 m")
insee_pop <- disaggregate(insee_pop, 4) / 16

# Save the disaggregated raster for reference
path <- file.path(insee_dir, "insee_population_50m.tif")
paste("Saving disaggregated raster to", path) %>% report
writeRaster(insee_pop, path, overwrite = TRUE)

# Convert to a velox object
report("Loading with velox")
insee_pop <- velox(insee_pop)

# Extract the total population of each 1 km square buffer
report("Extracting population")
sum_pop <- function(x) { sum(x, na.rm = TRUE) }
insee_pop <- parallel_extract(insee_pop, squares, sum_pop, ncores)

# Add the modis grid id, transform to a data frame, and save
path <- file.path(extracts_dir, "modis_1km_population.rds")
paste("Saving to", path) %>% report
data.frame(
  "modis_grid_id" = squares$id,
  "population" = insee_pop
) %>% saveRDS(., path)

report("Done")
