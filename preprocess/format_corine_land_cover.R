# Format corine land cover data
#
# For each land cover data year
# * Disaggregate to 50 m

library(magrittr)   # %>% pipe-like operator
library(sp)         # classes and methods for spatial data
library(raster)     # methods to manipulate gridded spatial data

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")

report("")
report("Formatting Corine Land Cover data")
report("---")

# Set the directory containing preprocessed corine land cover data
# The data was been mosaiced and clipped to France with a 1 km buffer in ArcGIS
clc_dir <- file.path(constants$work_dir, "corine_land_cover")
dir.create(clc_dir, showWarnings = FALSE)

# For each land cover data year, load the data and disaggregate to 50 m
for (year in constants$clc_years) {
  paste("Loading CLC", year, "data") %>% report
  clc_data <- paste0("clc_", year, "_france.tif") %>% file.path(clc_dir, .) %>% raster

  # Disaggregate from 100 m to 50 m
  report("  Disaggregating to 50 m")
  clc_data <- disaggregate(clc_data, 2)

  # Save the disaggregated data
  path <- paste0("clc_", year, "_france_50m.tif") %>% file.path(clc_dir, .)
  paste("  Saving to", path) %>% report
  writeRaster(clc_data, path, overwrite = TRUE)
}

report("Done")
