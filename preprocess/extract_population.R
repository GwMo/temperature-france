# Extract population density data using the reference grid

library(magrittr) # %>% pipe-like operator
library(parallel) # parallel computation
library(sp)       # classes and methods for spatial data
library(foreign)  # support for dbf files

data_dir <- file.path("~", "data") %>% path.expand
model_dir <- file.path("~", "temperature-france") %>% path.expand
output_dir <- file.path(model_dir, "data")
dir.create(output_dir, showWarnings = FALSE)
setwd(output_dir)

# Load helper functions
file.path(model_dir, "helpers", "report.R") %>% source
file.path(model_dir, "helpers", "parallel_over.R") %>% source

# Load the reference grid and save its original column names
report("Loading MODIS reference grid")
grid <- file.path(model_dir, "grids", "modis_grid.rds") %>% readRDS

# Load the 1 km square buffers
report("Loading 1 km square buffers")
squares <- file.path(model_dir, "buffers", "modis_square_1km.rds") %>% readRDS

# Use 16 cores for parallel extraction
ncores <- 16

##################
# INSEE POPULATION
##################

report("Extracting INSEE population density")

# Load the INSEE 200 m population data
report("  Loading INSEE 200 m population data")
insee_dir <- file.path(data_dir, "insee", "200m-carreaux-metropole")
insee_pop <- file.path(insee_dir, "car_m.dbf") %>% read.dbf(., as.is = TRUE)

# Extract the tile center coordinates from the tile ids
# The tile ids include the LAEA coordinates of the tile's bottom left corner so
# adding 100 gives the coordinates of the tile center
report("  Transforming to spatial points dataframe")
insee_pop$x <- substr(insee_pop$idINSPIRE, 24, 30) %>% as.numeric %>% + 100
insee_pop$y <- substr(insee_pop$idINSPIRE, 16, 22) %>% as.numeric %>% + 100

# Drop unneeded columns and convert to a spatial points data frame
insee_pop <- insee_pop[ , c("x", "y", "ind_c")]
names(insee_pop)[3] <- "population"
coordinates(insee_pop) <- ~ x + y
proj4string(insee_pop) <- "+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs"

# Calculate the total population of each 1 km square buffer
report("  Calculating population in 1 km square buffers")
grid$populations <- parallel_over(insee_pop, squares, sum, ncores)
grid$populations[is.na(grid$populations)] <- 0 # replace NA with 0

# Save the result
path <- file.path(output_dir, "modis_1km_population.rds")
paste("  Saving to", path) %>% report
saveRDS(grid@data, path)

report("Done")
