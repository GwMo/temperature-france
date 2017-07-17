# Format Meteo France meteorological station observations
#
# For each model year
# * Parse all observations for the year into a single data frame
# * Ensure there are no duplicates
# * FIXME TODO Add station type
# * Convert to a SpatialPointsDataFrame in EPSG:4326 and save

library(magrittr) # %>% pipe-like operator
library(dplyr)    # data manipulation e.g. joining tables
library(parallel) # parallel computation
library(sp)       # classes and methods for spatial data

# Set directories and load helper functions
file.path("~", "temperature-france", "helpers", "set_dirs.R") %>% source
file.path(helpers_dir, "report.R") %>% source
file.path(helpers_dir, "get_ncores.R") %>% source


report("Formatting Meteo France meteorological station observations")


# Detect the number of cores available
ncores <- get_ncores()

obs_dir <- file.path(data_dir, "meteo_france", "observations")
metadata <- file.path(obs_dir, "file_format.csv") %>% read.csv(., as.is = TRUE)
region_dirs <- list.files(obs_dir, pattern = "region\\d", full.names = TRUE)

# Create a directory to hold the formatted data
formatted_dir <- file.path(extracts_dir, "meteo_data")
dir.create(formatted_dir, showWarnings = FALSE)

# Define a function to parse latitude and longitude from the data files
# The files contain DDMMSS without leading zeros e.g. -00°00'36" -> -36
ddmmss_to_decimal <- function(ddmmss_as_integer) {
  # -13036 -> "-013036"
  ddmmss <- sprintf("% 07d", ddmmss_as_integer)

  # Convert the sign, degrees, minutes, and seconds their decimal value
  mult <- substr(ddmmss, 1, 1) %>% paste0(., "1") %>% as.numeric #  "-" -> -1
  degs <- substr(ddmmss, 2, 3) %>% as.numeric                    # "01" ->  1
  mins <- substr(ddmmss, 4, 5) %>% as.numeric /   60             # "30" ->  0.5
  secs <- substr(ddmmss, 6, 7) %>% as.numeric / 3600             # "36" ->  0.01

  # -1 * (1 + 0.05 + 0.01) -> -1.51
  mult * (degs + mins + secs)
}

# Iterate over model years
for (year in 2000:2016) {
  paste("Processing", year) %>% report

  # Parse the data for all regions into a single data frame
  report("  Loading all observations")
  obs <- mclapply(region_dirs, function(region_dir) {
    # Load the region data for the year
    station_data <-
      basename(region_dir) %>%
      paste0("donnees_", ., "_", year, "0101_", year, "1231.txt") %>%
      file.path(region_dir, .) %>%
      read.csv(
        .,
        as.is = TRUE,
        col.names = metadata$colname,
        colClasses = metadata$coltype,
        header = FALSE,
        sep = ";",
      )

    # Drop an empty column due to all lines ending in a semicolon
    station_data <- select(station_data, -empty)

    # Parse latitude and longitude to decimal degrees
    station_data$latitude  <- ddmmss_to_decimal(station_data$latitude)
    station_data$longitude <- ddmmss_to_decimal(station_data$longitude)

    # Parse the date but keep it as a character
    station_data$date <- as.Date(station_data$date, "%d%m%Y") %>% as.character

    # Return the parsed data
    station_data
  }, mc.cores = ncores) %>% do.call(rbind, .) %>% as.data.frame

  # Confirm that each station has a single latitude and longitude and no more
  # than one set of observations per date
  report("  Checking for duplicates")
  obs_checks <-
    group_by(obs, insee_id) %>%
    summarise(
      lats = length(unique(latitude)),  # unique latitudes per station
      lons = length(unique(longitude)), # unique longitudes per station
      days = length(unique(date)),      # unique dates per station
      rows = length(date)               # rows of data per station
    )
  all(obs_checks$lats == 1)               %>% stopifnot
  all(obs_checks$lons == 1)               %>% stopifnot
  all(obs_checks$days == obs_checks$rows) %>% stopifnot
  rm(obs_checks)

  # FIXME TODO add station type (after resolving disagreement between alternate
  # sources of station type)

  # Convert to a SpatialPointsDataFrame in EPSG:4326 projection
  report("  Converting to SpatialPointsDataFrame")
  coordinates(obs) <- ~ latitude + longitude
  proj4string(obs) <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"

  # Save the formatted data
  path <- paste0("meteo_data_", year, ".rds") %>% file.path(formatted_dir, .)
  paste("  Saving to", path) %>% report
  saveRDS(obs, path)

  # Cleanup
  rm(obs, path, year)
}

report("Done")
