# Extract land cover data using the reference grid

library(magrittr) # %>% pipe-like operator
library(parallel) # parallel computation
library(sp)       # classes and methods for spatial data
library(raster)   # methods to manipulate gridded spatial data
library(velox)    # c++ accelerated raster manipulation

data_dir <- file.path("~", "data") %>% path.expand
model_dir <- file.path("~", "temperature-france") %>% path.expand
output_dir <- file.path(model_dir, "data")
dir.create(output_dir, showWarnings = FALSE)
setwd(output_dir)

# Load helper functions
file.path(model_dir, "helpers", "report.R") %>% source
file.path(model_dir, "helpers", "parallel_extract.R") %>% source

# Load the reference grid and save its original column names
report("Loading MODIS reference grid")
grid <- file.path(model_dir, "grids", "modis_grid.rds") %>% readRDS

# Load the 1 km square buffers
report("Loading 1 km square buffers")
squares <- file.path(model_dir, "buffers", "modis_square_1km.rds") %>% readRDS

# Use 16 cores for parallel extraction
ncores <- 16

###################
# CORINE LAND COVER
###################

# Set the Corine Land Cover data dir
clc_dir <- file.path(data_dir, "copernicus", "corine_land_cover", "france")

# Load a dictionary to translate from land cover codes to data values
clc_dict <- file.path(clc_dir, "clc_legend.csv") %>% read.csv

# Define groups of land cover classes
clc_groups <- list(
  c(111),                     # Continuous urban
  c(112),                     # Discontinuous urban
  c(121, 122, 123, 124),      # Industrial, commercial, transportation
  c(131, 132, 133),           # Mines, dumps, construction sites
  c(141, 142),                # Green urban areas, sports / leisure facilities
  c(211, 212, 213),           # Arable land
  c(221, 222, 223),           # Vineyards, plantations, olive groves
  c(241, 242, 243),           # Heterogeneous agriculture
  c(311, 312, 313),           # Forest
  c(231, 321, 322),           # Pastures, grasslands, moors
  c(323, 324),                # Brush, shrublands
  c(331, 332, 333, 334),      # Sand, rock, sparse vegetation, burnt areas
  c(335),                     # Glaciers, snow
  c(411, 412, 421, 422, 423), # Wetlands
  c(511, 512, 521, 522, 523)  # Water
)
names(clc_groups) <- c(
  "urban_continuous",
  "urban_discontinuous",
  "commercial_industrial",
  "mines_dumps",
  "urban_green_leisure",
  "arable_land",
  "permanent_crops",
  "mixed_agricultural",
  "forest",
  "grassland_pasture",
  "shurbland",
  "bare",
  "snow",
  "wetland",
  "water"
)

# Extract the area for each group of land cover classes in 2000, 2006, and 2012
for (year in c(2000, 2006, 2012)) {
  paste("Extracting Corine Land Cover", year, "data") %>% report

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
  }) %>% cbind(grid@data, .)

  # Save the result and clear memory
  path <- paste("modis_1km_clc", year, sep = "_") %>% file.path(output_dir, .)
  paste("  Saving to", path) %>% report
  saveRDS(result, path)
  rm(clc_data, vx, result)
}

report("Done")
