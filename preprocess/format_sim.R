# Format SIM surface model output data to facilitate extraction
#
# For each annual data file
# * Load the data
# * Transform to a SpatialPixelsDataFrame
# * For each variable of interest
#   - Create and save a RasterStack with a separate layer for each day of data

library(magrittr)   # %>% pipe-like operator
library(parallel)   # parallel computation
library(data.table) # fast loading of data files
library(sp)         # classes and methods for spatial data
library(raster)     # methods to manipulate gridded spatial data

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")

report("")
report("Formatting Meteo France SIM surface model output")
report("---")

# Detect the number of cores available
ncores <- get_ncores()

################################
# METEO FRANCE SIM SURFACE MODEL
################################

# Create a directory to store the formatted data
processed_dir <- file.path(constants$work_dir, "meteo_france", "sim")
dir.create(processed_dir, showWarnings = FALSE)

# List the annual SIM data files and load the metadata file
sim_dir <- file.path(constants$data_dir, "meteo_france", "sim")
data_files <- list.files(sim_dir, full.names = TRUE, pattern = "^SIM2_\\d{4}_\\d{4,6}\\.csv$")
metadata <- file.path(sim_dir, "sim2_file_format.csv") %>% read.csv(., as.is = TRUE)

# List the variables we want
vars <- c(
  "temperature_min",
  "temperature_mean",
  "temperature_max",
  "humidity_specific",
  "humidity_relative",
  "precip_liquid",
  "precip_solid",
  "wind_speed"
)

# List the columns we want (coordinates + date + vars) and look up their actual column names
long_names <- c("x", "y", "date", vars)
select_cols <- match(long_names, metadata$longname) %>% metadata$colname[.]

# Process the raw SIM data files
for (sim_path in data_files) {
  paste("Loading", basename(sim_path)) %>% report

  # Read the data header and look up column classes from the metadata
  header <- fread(sim_path, header = FALSE, nrow = 1) %>% as.character
  col_classes <- match(header, metadata$colname) %>% metadata$coltype[.]

  # Read the data and set friendly column names
  sim <- fread(sim_path, colClasses = col_classes, select = select_cols, col.names = long_names)

  # Convert coordinates to metres
  report("  Converting coordinates from hectometres to metres")
  sim[, x := x * 100] # hectometres -> metres
  sim[, y := y * 100] # hectometres -> metres

  # Split the data by year
  report("  Splitting by year")
  year_splits <- split(sim, substr(sim$date, 1, 4))
  rm(sim) # remove the unsplit data from memory

  # Process the data for each year
  for (year in names(year_splits)) {
    paste("  Processing", year) %>% report

    # For each variable, create and save a RasterStack with a layer for each date
    # The most efficient way to do this is to create a RasterStack for each date containing the data
    # for all variables, then for each variable take the appropriate layer from each RasterStack
    report("    Converting to RasterStacks")

    # Split the dataset by date, and for each date create a RasterStack with one layer per variable
    # See note in helpers/constants.R about Lambert II étendu projection
    # -> 365 RasterStacks, each with length(vars) layers
    stacks <-
      year_splits[[year]] %>%           # get the data for the year
      split(., .$date) %>%              # split by date
      mclapply(function(slice, proj4) { # for each date
        coordinates(slice) <- ~ x + y   #   make a SpatialPointsDataFrame
        proj4string(slice) <- proj4     #   set the projection
        gridded(slice) <- TRUE          #   convert to SpatialPixelsDataFrame
        stack(slice)                    #   convert to RasterStack
      }, mc.cores = ncores, proj4 = constants$lambert_etendu)
    year_splits[[year]] <- NULL         # remove the data from memory

    # For each variable, create a RasterStack with one layer for each day and save as a tif
    # -> length(vars) tif files, each with 365 layers
    paste("    Saving to", processed_dir) %>% report
    for (v in vars) {
      stck <- lapply(stacks, function(stck) { # for each RasterStack (all data for one date)
        subset(stck, v)                       #   get the layer with data for the variable
      }) %>% stack                            # stack the 365 layers

      # Save as a tif file
      path <- paste0("SIM2_", year, "-", v, ".tif") %>% file.path(processed_dir, .)
      paste0("      ", basename(path)) %>% report
      writeRaster(stck, path, overwrite = TRUE)
      rm(stck)
    }
    rm(stacks)
  }
}

report("Done")


# library(ncdf4)
#
# FIXME TODO need to reverse and transform raster data before saving as netCDF?
#
# test <- stacks[[3]]
# v <- vars[3]
# nc_x <- ncdim_def(name = "easting", units = "metre", vals = unique(coordinates(test)[, "x"]))
# nc_y <- ncdim_def(name = "northing", units = "metre", vals = unique(coordinates(test)[, "y"]))
# nc_z <- ncdim_def(
#   name = "date",
#   units = "days since 1970-01-01",
#   vals = names(test) %>% as.Date("X%Y%m%d") %>% as.integer,
#   unlim = TRUE,
#   calendar = "gregorian"
# )
# # nc_vars <- lapply(vars, function(v) {
# nc_v <-
#   ncvar_def(
#     name = v,
#     units = "degC",
#     dim = list(nc_x, nc_y, nc_z)
#   )
# # })
# nc_out <- nc_create("../test.nc", vars = nc_v, force_v4 = TRUE)
# # lapply(vars, function(v) {
# ncvar_put(nc_out, nc_v, as.array(stacks[[v]]), verbose = TRUE)
# # })
# ncatt_put(nc_out, 0, "proj4string", etendu, prec = "text")
# ncatt_put(nc_out, 0, "crs_format", "PROJ.4", prec = "text")
# nc_close(nc_out)
