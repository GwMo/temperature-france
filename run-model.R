# Run the temperature model

library(magrittr)

# Setup -----------------------------------------------------------------------

# Load constants and helper functions
source("helpers/constants.R")
source("helpers/report.R")

# Create a logfile
logdir <- file.path(constants$code_dir, "log")
dir.create(logdir, showWarnings = FALSE)
constants$logfile <- Sys.time() %>% format("%Y-%m-%d_%H%M%S.log") %>% file.path(logdir, .)

# Function to run scripts, logging output to logfile and stopping on error
run_and_log <- function(script) {
  result <- paste0(script, " --log=", constants$logfile) %>% system2("Rscript", .)
  if (result != 0) {
    paste("Error in", script) %>% report
    stop()
  }
}

report("*** Creating the LST 1 km grid ***")
run_and_log("grid_setup/check_modis_alignment.R") # Check alignment of MODIS NDVI and LST data
run_and_log("grid_setup/create_modis_grid.R")     # Create a 1 km reference grid from the MODIS LST data
run_and_log("grid_setup/create_modis_buffers.R")  # Create buffers around the LST 1 km grid centroids

report("")
report("*** Preprocessing data ***")
run_and_log("preprocess/format_insee_population.R")  # Rasterize and disaggregate INSEE population
run_and_log("preprocess/format_corine_land_cover.R") # Disaggregate corine land cover
run_and_log("preprocess/format_sim.R")               # Parse Meteo France surface model output
run_and_log("preprocess/parse_meteo_data.R")         # Parse Meteo France weather station data
# run_and_log("preprocess/explore_meteo_data.R")     # Check the station data for outliers
run_and_log("preprocess/clean_meteo_data.R")         # Remove suspicious station data

report("")
report("*** Extracting data to the LST 1 km grid ***")
run_and_log("extract/extract_elevation.R")    # Elevation
run_and_log("extract/extract_land_cover.R")   # Land cover
run_and_log("extract/extract_population.R")   # Population
run_and_log("extract/extract_climate_type.R") # Climate type
run_and_log("extract/extract_sim.R")          # Meteo France surface model
run_and_log("extract/extract_modis_ndvi.R")   # MODIS NDVI
run_and_log("extract/extract_modis_lst.R")    # MODIS LST

report("")
report("Joining extracted datasets")
run_and_log("extract/join_data.R")

report("")
report("Fitting models")
run_and_log("model/fit_mod1.R")
