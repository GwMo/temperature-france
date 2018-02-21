# Clean the Meteo France station data -----------------------------------------

library(magrittr)   # pipe-like operators
library(parallel)   # parallel computation
library(sp)         # classes and methods for spatial data
library(raster)     # methods to manipulate gridded spatial data
library(dplyr)      # data manipulation e.g. joining tables
library(data.table) # fast data manipulation

source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")

report("")
report("Cleaning Meteo France station data")
report("---")

# Setup -----------------------------------------------------------------------

# An empty vector to hold a description of each cleaning step
step_descriptions <- character()

# Number of cores to use for parallel computation
ncores <- (length(constants$model_years) + 1) %>% min(., get_ncores())

# The directories for raw and processed station data
raw_dir <- file.path(constants$data_dir, "meteo_france", "observations")
processed_dir <- file.path(constants$work_dir, "meteo_france", "observations")

# Function definitions --------------------------------------------------------

# Function to calculate and report total observations and number just removed
tally_and_report <- function(old_tally = 0) {
  new_tally <- nrow(obs)
  msg <- paste(" ", new_tally, "observations")
  if (new_tally < old_tally) {
    msg <- paste0(msg, " (", old_tally - new_tally, " removed)")
  }
  report(msg)
  new_tally
}

# Function to calculate and save the number of observations per station location
save_station_stats <- function(dt, out_dir, filename) {
  report("Calculating number of observations per station")

  obs_counts <- dt[, .N, by = .(insee_id, latitude, longitude)]
  min_counts <- dt[!is.na(temperature_min), .N, by = .(insee_id, latitude, longitude)]
  mean_counts <- dt[!is.na(temperature_mean), .N, by = .(insee_id, latitude, longitude)]
  max_counts <- dt[!is.na(temperature_max), .N, by = .(insee_id, latitude, longitude)]
  obs_counts %>% setnames(old = "N", new = "obs")
  obs_counts[min_counts, tmin_obs := i.N, on = c("insee_id", "latitude", "longitude")]
  obs_counts[mean_counts, tmean_obs := i.N, on = c("insee_id", "latitude", "longitude")]
  obs_counts[max_counts, tmax_obs := i.N, on = c("insee_id", "latitude", "longitude")]

  # Add the number of locations per station
  location_counts <- obs_counts[, .N, by = .(insee_id)]
  obs_counts[location_counts, station_locations := i.N, on = "insee_id"]
  rm(location_counts)

  # Save as a csv
  path <- file.path(out_dir, filename)
  paste("  Saving to", path) %>% report
  fwrite(obs_counts, path)
}

# Function to split data by year and save
save_by_year <- function(df, out_dir, filename_suffix) {
  paste("Saving", filename_suffix, "observations to", out_dir) %>% report

  # Split by year and save in parallel
  split(df, df$year) %>%
    mclapply(function(splt, filename_suffix, out_dir) {
      splt$year[1] %>%
        paste0("meteo_data_", ., "-", filename_suffix, ".rds") %>%
        file.path(out_dir, .) %>%
        saveRDS(splt, .)
    }, filename_suffix = filename_suffix, out_dir = out_dir, mc.cores = ncores)

  report("  OK")
}

# Function to write a summary of the cleaning steps to a readme file
write_readme <- function(step_descriptions, out_dir) {
  paste("Writing readme in", out_dir) %>% report
  c(
    "Cleaned by removing observations that are:",
    paste("*", step_descriptions)
  ) %>%
  write(file.path(clean_dir, "readme.txt"))
}


# Load observations for all years ---------------------------------------------

paste(
  "Loading parsed observations for", first(constants$model_years), "to", last(constants$model_years)
) %>% report
obs <- mclapply(constants$model_years, function(year) {
  paste0("meteo_data_", year, "-parsed.rds") %>% file.path(processed_dir, "parsed", .) %>% readRDS
}, mc.cores = ncores) %>% do.call(rbind, .) %>% as.data.table
obs %>% setkeyv(c("date", "insee_id", "latitude", "longitude"))
# 15 seconds

# Create an empty data frame to hold removed observations
removed <- obs[0, ]

obs_tally <- tally_and_report()
# 15296681


# Calculate and save the number of observations per station location ----------
save_station_stats(obs, processed_dir, "stations_before_cleaning.csv")


# Add year, month, and day of year columns to facilitate filtering ------------

report("Adding year, month, and day of year columns for filtering")
dates <- as.Date(obs$date)
obs[, year := format.Date(dates, "%Y") %>% as.integer]
obs[, month := format.Date(dates, "%m") %>% as.integer]
obs[, doy := format.Date(dates, "%j") %>% as.integer]
# 50 seconds


# Add station type from Meteo France station metadata -------------------------

report("Adding station type")

# Load the metadata file layout
layout <- file.path(raw_dir, "station_metadata_file_layout.csv") %>% read.csv(as.is = TRUE)

# Read station type from the metadata and add it to the observations
metadata <-
  file.path(raw_dir, "station_metadata.csv") %>%
  fread(col.names = layout$colname, colClasses = layout$coltype)
metadata[, type := factor(type)]
obs[metadata, station_type := i.type, on = "insee_id"]
rm(layout, metadata)


# Add elevation from EU-DEM v1.1 based on observation locations ---------------

report("Adding elevation")

# Extract unique observation locations and transform to SpatialPoints
obs_pts <-
  obs %>%
  dplyr::select(longitude, latitude) %>% # SpatialPoints needs coords as Easting, Northing
  distinct %>%
  SpatialPoints(proj4string = CRS("+proj=longlat +datum=WGS84 +no_defs"))

# Load the Copernicus EU-DEM v1.1 elevation data
elevation <-
  file.path(constants$work_dir, "elevation", "eu-dem_v1-1", "eu_dem_v11_france.tif") %>%
  raster

# Extract elevation for each observation location
obs_pts$elevation <-
  spTransform(obs_pts, projection(elevation)) %>% # project to match elevation data
  raster::extract(elevation, .)                   # 40 seconds; velox is much slower

# Add elevation to the observations data frame
obs[as.data.table(obs_pts), station_elevation := i.elevation, on = c("latitude", "longitude")]
rm(elevation)


# Remove observations outside the study area ----------------------------------

description <- "outside study area"
paste("Removing observations", description) %>% report

# Flag locations as inside or outside study area (including a 1 km buffer)
study_area <-
  file.path(constants$work_dir, "borders", "france_buf1km_epsg-4326.shp") %>%
  shapefile
obs_pts$outside <- obs_pts %>% over(study_area) %>% is.na %>% as.logical
paste("  Found", length(obs_pts[obs_pts$outside, ]), "locations outside study area") %>% report
# 126 locations outside

# Add the flag to the observations
obs[as.data.table(obs_pts), outside := i.outside, on = c("latitude", "longitude")]

# Remember observations outside study area
removed <-
  obs[(outside), ] %>%
  .[, problem := description] %>%
  .[, outside := NULL] %>%
  bind_rows(removed, .)

# Remove observations outside the study area and the flag column
obs <- obs[!(outside), ]
obs[, outside := NULL]
step_descriptions %<>% c(description)

obs_tally <- tally_and_report(obs_tally)
#   342756 removed
# 14953925 total

rm(obs_pts, study_area, description)


# Remove observations from unknown stations -----------------------------------

description <- "from unknown station"
paste("Removing observations", description) %>% report

# Remember observations with no known station type
removed <-
  obs[station_type %>% as.character %>% is.na, ] %>%
  .[, problem := description] %>%
  bind_rows(removed, .)

# Remove observations missing min or max
obs <- obs[station_type %>% as.character %>% is.na %>% not, ]
step_descriptions %<>% c(description)

obs_tally <- tally_and_report(obs_tally)
#  1398130 removed
# 13555795 total

rm(description)


# Remove low-quality observations from co-located stations --------------------

description <- "co-located with a higher-quality station"
paste("Removing observations", description) %>% report

# Select observations from stations that have the same location as another station
colocated <-
  obs[, .N, by = c("date", "latitude", "longitude")] %>%
  .[N > 1, ] %>%
  obs[., on = c("date", "latitude", "longitude")]

# For each location and date, identify the lowest station type (should be highest quality)
colocated[
  ,
  best_station_type := levels(.SD$station_type)[min(as.integer(.SD$station_type))],
  by = c("date", "latitude", "longitude")
]

# Mark all observations from lower-quality co-located stations
obs[
  colocated[station_type != best_station_type, c("date", "insee_id"), with = FALSE],
  low_quality := TRUE,
  on = c("date", "insee_id")
]

# Remember observations from lower-quality co-located stations
obs[(low_quality), problem := description]
removed <-
  obs[(low_quality), ] %>%
  .[, problem := description] %>%
  .[, low_quality := NULL] %>%
  bind_rows(removed, .)

# Remove observations from lower-quality co-located stations and the flag column
obs <- obs[(is.na(obs$low_quality)), ]
obs[, low_quality := NULL]
step_descriptions %<>% c(description)

obs_tally <- tally_and_report(obs_tally)
#    44550 removed
# 13511245 total


# Remove inconsistent observations from co-located stations -------------------

description <- "from co-located stations with temperature discrepancy > 2 degrees"
paste("Removing observations", description) %>% report

# Select colocated observations that have the same station type
colocated <-
  colocated[station_type == best_station_type, ] %>%
  .[, .N, by = c("date", "latitude", "longitude")] %>%
  .[N > 1] %>%
  obs[., on = c("date", "latitude", "longitude")]

# Get the date and insee id of co-located observations whose temperatures differ by > 2 degrees
bad <-
  colocated[
    ,
    lapply(.SD, function(x) { max(x, na.rm = TRUE) - min(x, na.rm = TRUE) }),
    by = c("date", "latitude", "longitude"),
    .SDcols = c("temperature_min", "temperature_max")
  ] %>%
  .[temperature_min > 2 | temperature_max > 2, ] %>%
  colocated[., on = c("date", "latitude", "longitude")] %>%
  .[, c("date", "insee_id"), with = FALSE]

# Mark the observations from the bad station dates
obs[bad, tdiff := TRUE, on = c("date", "insee_id")]

# Remember the bad observations
obs[(tdiff), problem := description]
removed <-
  obs[(tdiff), ] %>%
  .[, tdiff := NULL] %>%
  bind_rows(removed, .)

# Remove the bad observations and flag column
obs <- obs[(is.na(tdiff)), ]
obs[, tdiff := NULL]
step_descriptions %<>% c(description)

obs_tally <- tally_and_report(obs_tally)
#     1502 removed
# 13509743 total

# Cleanup
rm(bad, colocated, description)


# Remove observations from stations with < 21 observations in a month ---------

description <- "from station with < 21 observations this month"
paste("Removing observations", description) %>% report

# Calculate the number of observations per station for each month
obs[, .N, by = c("year", "month", "insee_id")] %>%
  obs[., station_obs_this_month := i.N, on = c("year", "month", "insee_id")]

# Remember observations from stations with < 21 observations in a month
removed <-
  obs[station_obs_this_month < 21, ] %>%
  .[, problem := description] %>%
  .[, station_obs_this_month := NULL] %>%
  bind_rows(removed, .)

# Remove observations from stations with < 21 observations in a month
obs <- obs[station_obs_this_month >= 21, ]
obs[, station_obs_this_month := NULL]
step_descriptions %<>% c(description)

obs_tally <- tally_and_report(obs_tally)
#    29834 removed
# 13479909 total

# Cleanup
rm(description)


# Calculate and save the number of observations per station location ----------
save_station_stats(obs, processed_dir, "stations_after_cleaning.csv")


# Save the removed and the clean data -----------------------------------------

# # Save removed data
# file.path(processed_dir, "removed") %T>%
#   dir.create(recursive = TRUE, showWarnings = FALSE) %>%
#   save_by_year(removed, ., "removed")
#
# # Save clean data
# obs[, problem := NULL]
# clean_dir <- file.path(processed_dir, "clean")
# dir.create(clean_dir, recursive = TRUE, showWarnings = FALSE)
# save_by_year(obs, clean_dir, "clean")
#
# # Save a readme describing how the data was cleaned
# write_readme(step_descriptions, clean_dir)

# Save a list of all the stations

report("Done")
