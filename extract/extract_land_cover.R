# Extract land cover data for the reference grid cells
#
# For each land cover dataset
# * Load the disaggregated data as a velox object
# * Extract by 1 km square buffers
# * Add LST 1 km id and save

library(magrittr)   # %>% pipe-like operator
library(parallel)   # parallel computation
library(data.table) # fast data manipulation
library(sp)         # classes and methods for spatial data
library(raster)     # methods to manipulate gridded spatial data
library(velox)      # c++ accelerated raster manipulation

# Set directories and load helper functions
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")
source("helpers/stop_if_exists.R")
source("helpers/split_into_n.R")
source("helpers/parallel_velox_extract.R")
source("helpers/save_as_geotiff.R")

report("")
report("Extracting Corine Land Cover classes")
report("---")

# Setup -----------------------------------------------------------------------

# Detect the number of cores available
ncores <- get_ncores()

# Load the 1 km square buffers
report("Loading 1 km square buffers")
bufs <- file.path(constants$grid_lst_1km_dir, "grid_lst_1km_square_1km.rds") %>% readRDS

# Save the grid ids and indexes
grid_ids_indexes <- as.data.table(bufs)[, .(lst_1km_id = id, index)]

# Split the buffers into ncores groups, then remove the unsplit buffers to save memory
splits <- split_into_n(bufs, ncores)
rm(bufs)

# Load the 1 km grid as pixels in sinusoidal projection
# We will use this save rasters of the extracted data for visualisation
pixels <- file.path(constants$grid_lst_1km_dir, "grid_lst_1km_sinu_pixels.rds") %>% readRDS

# Extract ---------------------------------------------------------------------

# Set the Corine Land Cover data dir
clc_dir <- file.path(constants$work_dir, "corine_land_cover")

# Load a dictionary to translate from land cover codes to data values
clc_dict <- file.path(clc_dir, "clc_legend.csv") %>% read.csv

# Define groups of land cover classes
clc_groups <- list(
  "urban_built" = c(111, 112,                 # Continuous urban, discontinuous urban
                    121, 122, 123, 124),      # Industrial, commercial, transportation
  "urban_green" = c(141, 142),                # Green urban areas, sports / leisure facilities
  "mine_dump"   = c(131, 132, 133),           # Mines, dumps, construction sites
  "field"       = c(211, 212, 213,            # Arable land (rotational crops)
                    231,                      # Pastures
                    321, 322),                # Grasslands, moors
  "shrub"       = c(221, 222, 223,            # Vineyards, plantations, olive groves
                    241, 242, 243, 244,       # Mixed crops, agro-forestry
                    323, 324),                # Brush, shrublands
  "forest"      = c(311, 312, 313),           # Forest
  "bare"        = c(331, 332, 333, 334, 335), # Sand, rock, sparse vegetation, burnt areas, snow
  "wetland"     = c(411, 412, 421, 422, 423), # Wetlands
  "water"       = c(511, 512, 521, 522, 523)  # Water
)

# Extract the percent area for each group of land cover classes in 2000, 2006, and 2012
for (year in constants$clc_years) {
  paste("Extracting CLC", year) %>% report

  # Stop if the data has already been extracted
  out_path <-
    paste0("grid_lst_1km_clc_", year, ".tif") %>%
    file.path(constants$grid_lst_1km_extracts_dir, .)
  stop_if_exists(out_path)

  # Load the data as a velox object
  clc_path <- paste0("clc_", year, "_france_50m.tif") %>% file.path(clc_dir, .)
  report("  Loading data with velox:")
  paste0("    ", clc_path)
  clc_vx <- velox(clc_path)
  rm(clc_path)

  # Extract the percent area occupied by each group of land cover classes
  result <- sapply(names(clc_groups), function(name) {
    paste("  Calculating area for", name) %>% report

    # Look up the data values corresponding to the group's land cover classes
    vals <- clc_dict$CLC_CODE %in% clc_groups[[name]] %>% clc_dict$GRID_CODE[.]

    # Calculate the percent of each buffer covered by the group classes
    clc_fun <- function(x) { mean(x %in% vals) }
    parallel_velox_extract(clc_data, splits, clc_fun, ncores)
  }) %>% as.data.table

  # Add the grid id
  result[, (names(grid_ids_indexes)) := grid_ids_indexes]
  setcolorder(result, c("lst_1km_id", "index", names(clc_groups)))

  # Save ------------------------------------------------------------------------
  paste("  Saving in", dirname(out_path)) %>% report

  # GeoTIFF
  tif_path <- sub("\\.rds$", ".tif", out_path)
  paste0("    ", basename(tif_path)) %>% report
  save_as_geotiff(result, pixels, tif_path)
  rm(tif_path)

  # RDS
  result %>% setkey("lst_1km_id")
  paste0("  ", basename(out_path)) %>% report
  saveRDS(result[, !"index"], out_path)
  rm(out_path)

  # Cleanup
  rm(clc_vx, result)
}

report("Done")
