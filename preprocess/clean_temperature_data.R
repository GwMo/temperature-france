# Clean the Meteo France station data -----------------------------------------

library(magrittr) # pipe-like operators
library(parallel) # parallel computation
library(sp)       # classes and methods for spatial data
library(raster)   # methods to manipulate gridded spatial data
library(dplyr)    # data manipulation e.g. joining tables

source("helpers/set_dirs.R")
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")

report("Cleaning Meteo France station data")


# Setup -----------------------------------------------------------------------

# An empty vector to hold a description of each cleaning step
step_descriptions <- character()

# Number of cores to use for parallel computation
ncores <- length(model_years) + 1 %>% min(., get_ncores())

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
    },
    filename_suffix = filename_suffix, out_dir = out_dir, mc.cores = ncores)
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

paste("Loading parsed observations for", first(model_years), "to", last(model_years)) %>% report

parsed_dir <- file.path(output_dir, "data_extracts", "meteo_france", "observations", "parsed")
obs <- mclapply(model_years, function(year) {
  paste0("meteo_data_", year, "-parsed.rds") %>% file.path(parsed_dir, .) %>% readRDS
}, mc.cores = ncores) %>% do.call(rbind, .)
rm(parsed_dir)
# 14 seconds

# Create an empty data frame to hold removed observations
removed <- obs[0, ]

obs_tally <- tally_and_report()
# 15296681


# Add year, month, and day of year columns to facilitate filtering ------------

report("Adding year, month, and day of year columns for filtering")

obs %<>% mutate(
  "year" = format.Date(date, "%Y") %>% as.integer,
  "month" = format.Date(date, "%m") %>% as.integer,
  "doy" = format.Date(date, "%j") %>% as.integer
)
# 14 seconds


# Add station type from Meteo France station metadata -------------------------

report("Adding station type")

# Load the metadata file layout
layout <-
  file.path(data_dir, "meteo_france", "observations", "station_metadata_layout.csv") %>%
  read.csv(as.is = TRUE)

# Read station type from the metadata and add it to the observations
obs <-
  file.path(data_dir, "meteo_france", "observations", "station_metadata.csv") %>%
  read.csv(as.is = TRUE, col.names = layout$colname, colClasses = layout$coltype) %>%
  mutate("station_type" = as.factor(type)) %>%
  dplyr::select(insee_id, station_type) %>%
  left_join(obs, ., by = "insee_id") %>%
  mutate("station_type" = addNA(station_type))
rm(layout)


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
  file.path(data_dir, "copernicus", "eu-dem", "france", "eu_dem_v11_france.tif") %>%
  raster

# Extract elevation for each observation location
obs_pts$elevation <-
  spTransform(obs_pts, projection(elevation)) %>% # project to match elevation data
  raster::extract(elevation, .)                   # 40 seconds; velox is much slower

# Add elevation to the observations data frame
obs$elevation <- left_join(
  dplyr::select(obs, latitude, longitude),
  as.data.frame(obs_pts),
  by = c("latitude", "longitude")
) %$% elevation

rm(elevation, obs_pts)


# Remove observations outside the study area ----------------------------------

description <- "outside study area"
paste("Removing observations", description) %>% report

# Extract unique observation coordinates
obs_pts <-
  obs %>%
  distinct(latitude, longitude) %>%
  dplyr::select(longitude, latitude) %>% # SpatialPoints needs coords as Easting, Northing
  SpatialPoints(proj4string = CRS("+proj=longlat +datum=WGS84 +no_defs"))

# Flag locations as inside or outside study area
study_area <-
  file.path(data_dir, "ign", "borders", "buffered", "france_buf1km_epsg-4326.shp") %>%
  shapefile
obs_pts$outside <- obs_pts %>% over(study_area) %>% is.na %>% as.logical
paste("  Found", length(obs_pts[obs_pts$outside, ]), "locations outside study area") %>% report
# 126 locations outside

# Add the flag to the observations
obs <- obs_pts %>% as.data.frame %>% left_join(obs, ., c("latitude", "longitude"))

# Remember observations outside study area
removed <-
  obs %>%
  filter(outside) %>%
  mutate("problem" = description) %>%
  dplyr::select(-outside) %>%
  bind_rows(removed, .)

# Remove observations outside the study area and the flag column
obs <- obs %>% filter(!outside) %>% dplyr::select(-outside)
step_descriptions %<>% c(description)

obs_tally <- tally_and_report(obs_tally)
#   342756 removed
# 15296681 total

rm(obs_pts, study_area, description)
detach("package:raster", unload = TRUE) # unload because raster masks some dplyr functions


# Remove observations from unknown stations -----------------------------------

description <- "from unknown station"
paste("Removing observations", description) %>% report

# Remember observations with no known station type
removed <-
  obs %>%
  filter(station_type %>% as.character %>% is.na) %>%
  mutate("problem" = description) %>%
  bind_rows(removed, .)

# Remove observations missing min or max
obs %<>% filter(station_type %>% as.character %>% is.na %>% not)
step_descriptions %<>% c(description)

obs_tally <- tally_and_report(obs_tally)
#  1398130 removed
# 13555795 total

rm(description)


# Remove observations from stations with < 21 observations in a month ---------

description <- "from station with < 21 observations this month"
paste("Removing observations", description) %>% report

# Calculate the number of observations per station for each month
obs %<>%
  group_by(year, month, insee_id) %>%
  summarize("station_obs_this_month" = n()) %>%
  left_join(obs, ., by = c("year", "month", "insee_id"))

# Remember observations from stations with < 21 observations in a month
removed <-
  obs %>%
  filter(station_obs_this_month < 21) %>%
  mutate("problem" = description) %>%
  bind_rows(removed, .)

# Remove observations from stations with < 21 observations in a month
obs %<>% filter(station_obs_this_month >= 21)
step_descriptions %<>% c(description)

obs_tally <- tally_and_report(obs_tally)
#    29182 removed
# 13526613 total

# Cleanup
obs %<>% select(-station_obs_this_month)
rm(description)


# Save the removed and the clean data -----------------------------------------

# Save removed data
output_dir %>%
  file.path("data_extracts", "meteo_france", "observations", "removed") %T>%
  dir.create(recursive = TRUE, showWarnings = FALSE) %>%
  save_by_year(removed, ., "removed")

# Save clean data
clean_dir <- file.path(output_dir, "data_extracts", "meteo_france", "observations", "clean")
dir.create(clean_dir, recursive = TRUE, showWarnings = FALSE)
save_by_year(obs, clean_dir, "clean")

# Save a readme describing how the data was cleaned
write_readme(step_descriptions, clean_dir)

# Cleanup ---------------------------------------------------------------------
rm(obs, removed, clean_dir, ncores, obs_tally, step_descriptions, save_by_year,
   tally_and_report, write_readme)

report("Done")
