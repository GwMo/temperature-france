# Parse Meteo France station data

library(magrittr) # %>% pipe-like operator
library(dplyr)    # data manipulation e.g. joining tables
library(parallel) # parallel computation

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")

report("")
report("Parsing Meteo France station data")
report("---")

# Function to parse latitude and longitude from the data files
# The files contain DDMMSS without leading zeros (e.g. -01°30'36" is stored as -13036)
ddmmss_to_decimal <- function(ddmmss_as_integer) {
  # Left pad with zeros, preserving any negative sign
  ddmmss <- sprintf("% 07d", ddmmss_as_integer) # -13036 -> "-013036"

  # Convert the sign, degrees, minutes, and seconds their decimal value
  mult <- substr(ddmmss, 1, 1) %>% paste0(., "1") %>% as.numeric #  "-" -> -1
  degs <- substr(ddmmss, 2, 3) %>% as.numeric                    # "01" ->  1
  mins <- substr(ddmmss, 4, 5) %>% as.numeric /   60             # "30" ->  0.5
  secs <- substr(ddmmss, 6, 7) %>% as.numeric / 3600             # "36" ->  0.01

  # Combine to a decimal value
  mult * (degs + mins + secs) # -1 * (1 + 0.5 + 0.01) -> -1.51
}

# Create a directory to hold the parsed data
parsed_dir <- file.path(constants$work_dir, "meteo_france", "observations", "parsed")
dir.create(parsed_dir, recursive = TRUE, showWarnings = FALSE)

# The directories containing the raw data (which is grouped by region)
region_dirs <-
  file.path(constants$data_dir, "meteo_france", "observations") %>%
  list.files(., pattern = "region\\d", full.names = TRUE)

# Load the data file layout
layout <-
  file.path(constants$data_dir, "meteo_france", "observations", "meteo_obs_file_layout.csv") %>%
  read.csv(., as.is = TRUE)

# Set the number of cores to use for parallel computation
ncores <-get_ncores()

# Parse and briefly check observations for all years
for (year in 2000:2016) {
  paste("Processing", year) %>% report

  # Load observations for all regions
  report("  Loading observations")
  obs <- mclapply(region_dirs, function(region_dir) {
    # Load the observations for the region
    station_data <-
      basename(region_dir) %>%
      paste0("donnees_", ., "_", year, "0101_", year, "1231.txt") %>%
      file.path(region_dir, .) %>%
      read.csv(
        .,
        as.is = TRUE,
        col.names = layout$colname,
        colClasses = layout$coltype,
        header = FALSE,
        sep = ";"
      )

    # Drop rightmost column (which is always empty)
    station_data <- select(station_data, -empty)

    # Parse latitude and longitude to decimal degrees
    station_data$latitude  <- ddmmss_to_decimal(station_data$latitude)
    station_data$longitude <- ddmmss_to_decimal(station_data$longitude)

    # Parse the date
    station_data$date <- as.Date(station_data$date, "%d%m%Y") %>% format("%Y-%m-%d")

    # Return the parsed data
    station_data
  }, mc.cores = ncores) %>% do.call(rbind.data.frame, .)

  # Ensure that all observations fall within the data year
  report("  Checking for out-of-period observations")
  stopifnot(paste0(year, "-01-01") <= min(obs$date))
  stopifnot(paste0(year, "-12-31") >= max(obs$date))

  # Ensure that each station has only one set of observations per date
  report("  Checking for duplicate observations")
  obs %>%
    group_by(insee_id) %>%
    summarise(
      "dates" = n_distinct(date),
      "records" = n()
    ) %$%
    stopifnot(all(dates == records))

  # Save the parsed data
  path <- paste0("meteo_data_", year, "-parsed.rds") %>% file.path(parsed_dir, .)
  paste("  Saving to", path) %>% report
  saveRDS(obs, path)

  # Cleanup
  rm(obs)
}

report("Done")
