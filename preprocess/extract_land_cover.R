# Extract land cover data for the reference grid cells
#
# For each land cover dataset
# * Load the data as a raster
# * Disaggregate to 50 m
# * Convert to a velox object
# * Extract by 1 km square buffers
# * Add the MODIS grid id and save

library(magrittr) # %>% pipe-like operator
library(parallel) # parallel computation
library(sp)       # classes and methods for spatial data
library(raster)   # methods to manipulate gridded spatial data
library(velox)    # c++ accelerated raster manipulation

# Set directories and load helper functions
file.path("~", "temperature-france", "helpers", "set_dirs.R") %>% source
file.path(helpers_dir, "report.R") %>% source
file.path(helpers_dir, "get_ncores.R") %>% source
file.path(helpers_dir, "parallel_extract.R") %>% source


report("Extracting Corine Land Cover data")


# Load the 1 km square buffers
report("Loading 1 km square buffers")
squares <- file.path(buffers_dir, "modis_square_1km.rds") %>% readRDS

# Detect the number of cores available
ncores <- get_ncores()

###################
# CORINE LAND COVER
###################

# Set the Corine Land Cover data dir
clc_dir <- file.path(data_dir, "copernicus", "corine_land_cover", "france")

# Load a dictionary to translate from land cover codes to data values
clc_dict <- file.path(clc_dir, "clc_legend.csv") %>% read.csv

# Define groups of land cover classes
clc_groups <- list(
  "urban_continuous"      = c(111),                     # Continuous urban
  "urban_discontinuous"   = c(112),                     # Discontinuous urban
  "commercial_industrial" = c(121, 122, 123, 124),      # Industrial, commercial, transportation
  "mines_dumps"           = c(131, 132, 133),           # Mines, dumps, construction sites
  "urban_green_leisure"   = c(141, 142),                # Green urban areas, sports / leisure facilities
  "arable_land"           = c(211, 212, 213),           # Arable land
  "permanent_crops"       = c(221, 222, 223),           # Vineyards, plantations, olive groves
  "mixed_agricultural"    = c(241, 242, 243),           # Heterogeneous agriculture
  "forest"                = c(311, 312, 313),           # Forest
  "grassland_pasture"     = c(231, 321, 322),           # Pastures, grasslands, moors
  "shurbland"             = c(323, 324),                # Brush, shrublands
  "bare"                  = c(331, 332, 333, 334),      # Sand, rock, sparse vegetation, burnt areas
  "snow"                  = c(335),                     # Glaciers, snow
  "wetland"               = c(411, 412, 421, 422, 423), # Wetlands
  "water"                 = c(511, 512, 521, 522, 523)  # Water
)

# Extract the area for each group of land cover classes in 2000, 2006, and 2012
for (year in c(2000, 2006, 2012)) {
  paste("Loading CLC", year, "data") %>% report

  # Load the raster data for the year
  # This file has already been mosaiced and clipped to France with a 1 km buffer
  filename <- paste("clc", year, "france.tif", sep = "_")
  clc_data <- file.path(clc_dir, filename) %>% raster

  # Disaggregate the data from 100 m to 50 m
  report("  Disaggregating to 50 m")
  clc_data <- disaggregate(clc_data, 2)

  # Create a velox object from the raster data
  report("  Loading data with velox")
  vx <- velox(clc_data)

  # Extract the area occupied by each group of land cover classes
  result <- sapply(names(clc_groups), function(name) {
    paste("  Calculating area for", name) %>% report

    # Look up the data values corresponding to the group's land cover classes
    vals <- clc_dict$CLC_CODE %in% clc_groups[[name]] %>% clc_dict$GRID_CODE[.]

    # Calculate the area of the group classes for each buffer
    parallel_extract(vx, squares, function(x) { mean(x %in% vals) }, ncores)
  })

  # Add the modis grid id, transform to data frame, and save
  path <- paste0("modis_1km_clc_", year, ".rds") %>% file.path(extracts_dir, .)
  paste("  Saving to", path) %>% report
  data.frame(
    "modis_grid_id" = squares$id,
    result
  ) %>% saveRDS(., path)

  # Clear memory
  rm(clc_data, vx, result)
}

report("Done")
