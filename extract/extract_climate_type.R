# Extract Joly (2010) climate type for the reference grid cells
#
# * Load the climate type data as a velox object
# * Assign a projection
# * Calculate predominant climate type for each 1 km square buffer
# * Add LST 1 km id and save

library(magrittr)   # %>% pipe-like operator
library(parallel)   # parallel computation
library(data.table) # fast data manipulation
library(sp)         # classes and methods for spatial data
library(raster)     # methods to manipulate gridded spatial data
library(velox)      # c++ accelerated raster manipulation

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")
source("helpers/stop_if_exists.R")
source("helpers/split_into_n.R")
source("helpers/parallel_velox_extract.R")
source("helpers/save_as_geotiff.R")

report("")
report("Extracting Joly (2010) climate type")
report("---")

# Stop if the data has already been extracted
out_path <- file.path(constants$grid_lst_1km_extracts_dir, "grid_lst_1km_joly_climate_type.rds")
stop_if_exists(out_path)

# Setup -----------------------------------------------------------------------

# Detect the number of cores available
ncores <- get_ncores()

# Load the 1 km square buffers
report("Loading 1 km square buffers")
bufs <- file.path(constants$grid_lst_1km_dir, "grid_lst_1km_square_1km.rds") %>% readRDS

# Save the LST 1 km ids and indexes
grid_ids_indexes <- as.data.table(bufs)[, .(lst_1km_id = id, index)]

# Split the buffers into ncores groups, then remove the unsplit buffers to save memory
splits <- split_into_n(bufs, ncores)
rm(bufs)

# Load the 1 km grid as pixels in sinusoidal projection
# We will use this to save rasters of the extracted data for visualisation
pixels <- file.path(constants$grid_lst_1km_dir, "grid_lst_1km_sinu_pixels.rds") %>% readRDS

# Extract ---------------------------------------------------------------------

# Load the climate type data as a velox object
path <- file.path(constants$work_dir, "climate_types", "joly_2010_climate_types_50m.tif")
report("Loading climate type data:")
paste0("  ", path) %>% report
type_vx <- velox(path)
rm(path)

# Determine the predominant climate type for each buffer
report("Extracting")
type_fun <- function(x) { modal(x, na.rm = TRUE) }
types <- parallel_velox_extract(type_vx, splits, fun = type_fun, ncores = ncores)
types <- as.factor(types)

# Add the grid id and index
result <- copy(grid_ids_indexes)
result[, climate_type := types]

# Save ------------------------------------------------------------------------
paste("Saving in", dirname(out_path)) %>% report

# GeoTIFF
tif_path <- sub("\\.rds$", ".tif", out_path)
paste0("  ", basename(tif_path)) %>% report
save_as_geotiff(result, pixels, tif_path)
rm(tif_path)

# RDS
result %>% setkey("lst_1km_id")
paste0("  ", basename(out_path)) %>% report
saveRDS(result[, !"index"], out_path)
rm(out_path)

report("Done")
