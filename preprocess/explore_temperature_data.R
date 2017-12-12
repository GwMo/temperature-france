# Explore the Meteo France station data ---------------------------------------

library(magrittr)   # %>% pipe-like operator
library(parallel)   # parallel computation
library(sp)         # classes and methods for spatial data
library(rgdal)      # wrapper for GDAL and proj.4 to manipulate spatial data
library(dplyr)      # data manipulation e.g. joining tables
library(tidyr)      # data tidying
library(ggplot2)    # plotting

# library(mapview)    # interactive maps
# library(data.table) # between function
# library(cowplot)    # aligning multiple plots
# library(plotly)   # interactive plots

# Load helper functions
source("helpers/set_dirs.R")
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")

report("Exploring clean Meteo France station data")


# Setup -----------------------------------------------------------------------

# Number of cores to use for parallel computation
ncores <- length(model_years) + 1 %>% min(., get_ncores())

# Create a directory to hold visualizations
vis_dir <- file.path(output_dir, "visualizations")
dir.create(vis_dir, showWarnings = FALSE)

# Function definitions --------------------------------------------------------

# Group and summarize data
group_and_summarize <- function(df, group_cols, vars) {
  smry <- lapply(vars, function(v_char) {
    v <- as.name(v_char) # v is passed as a string but needs to be a name for dplyr::summarise
    df %>%
      group_by_(.dots = group_cols) %>%
      summarize(
        "variable" = v_char,
        "n_obs"    = length(!is.na(!!v)),
        "min"      = min(     !!v, na.rm = TRUE),
        "mean"     = mean(    !!v, na.rm = TRUE),
        "max"      = max(     !!v, na.rm = TRUE),
        "sd"       = sd(      !!v, na.rm = TRUE),
        "q1"       = quantile(!!v, na.rm = TRUE, prob = 0.25),
        "median"   = median(  !!v, na.rm = TRUE),
        "q3"       = quantile(!!v, na.rm = TRUE, prob = 0.75),
        "iqr"      = q3 - q1
      )
  }) %>% bind_rows %>% ungroup # ungroup to avoid confusion

  # Convert variable to a factor to ensure ploting doesn't change display order of variables
  # this needs to happen after binding because factors cannot be bound together
  smry$variable <- factor(smry$variable, levels = vars)
  smry
}


# Load cleaned observations ---------------------------------------------

paste("Loading observations for", first(model_years), "to", last(model_years)) %>% report
clean_dir <- file.path(output_dir, "data_extracts", "meteo_france", "observations", "clean")
obs <- mclapply(model_years, function(year) {
  paste0("meteo_data_", year, "-clean.rds") %>% file.path(clean_dir, .) %>% readRDS
}, mc.cores = ncores) %>% do.call(rbind, .)
rm(clean_dir)


# Summarize the data --------------------------------------------------------------

report("Summarizing data")

# The variables to summarize
var_names <- c("temperature_min", "temperature_mean", "temperature_max")

report("  By date")
smry_by_date <- group_and_summarize(obs, "date", var_names)
# 18 seconds

report("  By day of year")
smry_by_doy <- group_and_summarize(obs, "doy", var_names)
# 12 seconds

report("  By month")
smry_by_month <- group_and_summarize(obs, "month", var_names)
# 8 seconds

report("  By year and month")
smry_by_year_month <- group_and_summarize(obs, c("year", "month"), var_names)
# 9 seconds

report("  By station")
smry_by_station <- group_and_summarize(obs, "insee_id", var_names)
# 20 seconds

report("  By year and station")
smry_by_year_station <- group_and_summarize(obs, c("year", "insee_id"), var_names)
# 85 seconds

report("  By station type")
smry_by_stype <- group_and_summarize(obs, "station_type", var_names)
# 9 seconds

report("  By year and station type")
smry_by_year_stype <- group_and_summarize(obs, c("year", "station_type"), var_names)
# 9 seconds

report("  By year, month, and station type")
smry_by_year_month_stype <- group_and_summarize(obs, c("year", "month", "station_type"), var_names)
# 10 seconds

report("  By year, month, and number of observations per station")
smry_by_year_month_obs_per_station <-
  obs %>%
  group_by(year, month, insee_id) %>%
  summarize("obs_per_station" = n()) %>%
  group_by(year, month, obs_per_station) %>%
  summarize("n_obs" = sum(obs_per_station)) %>%
  mutate("pct_of_obs_this_month" = n_obs / sum(n_obs))
# 2 seconds


# Plot the summaries -----------------------------------------------------------

paste("Plotting summaries to", vis_dir) %>% report

report("  Daily temperatures")
file.path(vis_dir, "meteo-daily.png") %>% png(., width = 11520, height = 1800)
p <-
  smry_by_date %>%
  ggplot(aes(x = date)) +
    geom_ribbon(aes(
      ymin = mean + (2 * sd),
      ymax = mean - (2 * sd)
    ), alpha = 0.5, fill = "grey") +
    geom_line(aes(y = min),  colour = "blue") +
    geom_line(aes(y = mean), colour = "black") +
    geom_line(aes(y = max),  colour = "red") +
    geom_line(aes(y = median - (1.5 * iqr)), colour = "blue",  linetype = "dashed") +
    geom_line(aes(y = median),               colour = "black", linetype = "dashed") +
    geom_line(aes(y = median + (1.5 * iqr)), colour = "red",   linetype = "dashed") +
    scale_x_date(
      breaks = seq.Date(min(obs$date), max(obs$date), by = "month"),
      expand = c(0, 0),
      name = element_blank()) +
    scale_y_continuous(breaks = seq(-40, 50, 10), name = "°C") +
    facet_grid(variable ~ .)
print(p)
dev.off()

report("  Daily normal temperatures")
file.path(vis_dir, "meteo-daily-normals.png") %>% png(., width = 1920, height = 900)
p <-
  smry_by_doy %>%
  ggplot(aes(x = doy)) +
    geom_ribbon(aes(
      ymin = mean + (2 * sd),
      ymax = mean - (2 * sd)
    ), alpha = 0.5, fill = "grey") +
    geom_line(aes(y = min),  colour = "blue") +
    geom_line(aes(y = mean), colour = "black") +
    geom_line(aes(y = max),  colour = "red") +
    geom_line(aes(y = median - (1.5 * iqr)), colour = "blue",  linetype = "dashed") +
    geom_line(aes(y = median),               colour = "black", linetype = "dashed") +
    geom_line(aes(y = median + (1.5 * iqr)), colour = "red",   linetype = "dashed") +
    scale_x_continuous(
      breaks = seq(0, 360, 30),
      expand = c(0, 0),
      name = "Day of Year"
    ) +
    scale_y_continuous(breaks = seq(-40, 50, 10), name = "°C") +
    facet_grid(variable ~ .)
print(p)
dev.off()

report("  Monthly normal temperatures")
file.path(vis_dir, "meteo-monthly-normals.png") %>% png(., width = 1920, height = 900)
p <-
  smry_by_month %>%
  ggplot(aes(x = month)) +
    geom_ribbon(aes(
      ymin = mean + (2 * sd),
      ymax = mean - (2 * sd)
    ), alpha = 0.5, fill = "grey") +
    geom_line(aes(y = min),  colour = "blue") +
    geom_line(aes(y = mean), colour = "black") +
    geom_line(aes(y = max),  colour = "red") +
    geom_line(aes(y = median - (1.5 * iqr)), colour = "blue",  linetype = "dashed") +
    geom_line(aes(y = median),               colour = "black", linetype = "dashed") +
    geom_line(aes(y = median + (1.5 * iqr)), colour = "red",   linetype = "dashed") +
    scale_x_continuous(breaks = seq(1, 12, 1)) +
    scale_y_continuous(breaks = seq(-40, 50, 10), name = "°C") +
    facet_grid(. ~ variable)
print(p)
dev.off()

report("  Annual monthly normal temperatures")
file.path(vis_dir, "meteo-annual-monthly-normals.png") %>% png(., width = 3600, height = 900)
p <-
  smry_by_year_month %>%
  ggplot(aes(x = month)) +
    geom_ribbon(aes(
      ymin = mean + (2 * sd),
      ymax = mean - (2 * sd)
    ), alpha = 0.5, fill = "grey") +
    geom_line(aes(y = min),  colour = "blue") +
    geom_line(aes(y = mean), colour = "black") +
    geom_line(aes(y = max),  colour = "red") +
    geom_line(aes(y = median - (1.5 * iqr)), colour = "blue",  linetype = "dashed") +
    geom_line(aes(y = median),               colour = "black", linetype = "dashed") +
    geom_line(aes(y = median + (1.5 * iqr)), colour = "red",   linetype = "dashed") +
    scale_x_continuous(breaks = seq(1, 12, 1)) +
    scale_y_continuous(breaks = seq(-40, 50, 10), name = "°C") +
    facet_grid(variable ~ year)
print(p)
dev.off()

report("  Observations per station histogram")
file.path(vis_dir, "meteo-obs_per_station-hist.png") %>% png(., width = 1920, height = 900)
p <-
  smry_by_station %>%
  ggplot(aes(n_obs)) +
    geom_histogram(binwidth = 365) +
    scale_x_continuous(
      breaks = seq.Date(
        from = min(model_years) %>% paste0("-01-01") %>% as.Date,
        to =   max(model_years) %>% paste0("-12-31") %>% as.Date,
        by = 1
      ) %>% format.Date("%Y") %>% table %>% as.numeric %>% cumsum %>% c(0, .),
      name = "Days with data"
    ) +
    scale_y_continuous(name = "Stations") +
    facet_grid(. ~ variable)
print(p)
dev.off()

report("  Observations per station boxplot")
file.path(vis_dir, "meteo-obs_per_station-box.png") %>% png(., width = 1920, height = 900)
p <-
  smry_by_station %>%
  ggplot(aes(variable, n_obs)) +
    geom_boxplot() +
    scale_y_continuous(breaks = seq(0, 6210, 365), name = "Days with data") +
    scale_x_discrete(name = element_blank(), position = "top")
print(p)
dev.off()

report("  Annual observations per station histogram")
file.path(vis_dir, "meteo-annual-obs_per_station-hist.png") %>% png(., width = 1920, height = 900)
p <-
  smry_by_year_station %>%
  ggplot(aes(n_obs)) +
    geom_histogram(binwidth = 30) +
    scale_x_continuous(breaks = seq(0, 360, 90), name = "Days with data") +
    scale_y_continuous(name = "Stations") +
    facet_grid(variable ~ year)
print(p)
dev.off()

report("  Annual observations per station boxplot")
file.path(vis_dir, "meteo-annual-obs_per_station-box.png") %>% png(., width = 1920, height = 900)
p <-
  smry_by_year_station %>%
  ggplot(aes(as.factor(year), n_obs)) +
    geom_boxplot() +
    scale_x_discrete(name = element_blank()) +
    scale_y_continuous(breaks = seq(0, 360, 30), name = "Days with data") +
    facet_grid(variable ~ .)
print(p)
dev.off()

report("  Annual cumulative share of observations by observations per station")
file.path(vis_dir, "meteo-annual-obs_per_station-cumpct.png") %>% png(., width = 1920, height = 900)
p <-
  smry_by_year_station %>%
  mutate("obs_per_station" = n_obs) %>%
  group_by(year, variable, obs_per_station) %>%
  summarize("n_obs" = sum(obs_per_station)) %>%
  mutate("cum_pct_obs" = cumsum(n_obs / sum(n_obs))) %>%
  ggplot(aes(x = obs_per_station)) +
    geom_area(aes(y = cum_pct_obs), alpha = 0.5) +
    scale_x_continuous(breaks = seq(0, 360, 90)) +
    scale_y_continuous(limits = c(0, 0.1)) +
    facet_grid(variable ~ year)
print(p)
dev.off()

report("  Share of observations by station type")
file.path(vis_dir, "meteo-obs_per_stype-pct.png") %>% png(., width = 1920, height = 900)
p <-
  smry_by_stype %>%
  group_by(variable) %>%
  summarize("total_obs" = sum(n_obs)) %>%
  left_join(smry_by_stype, by = "variable") %>%
  mutate("pct_obs" = n_obs / total_obs) %>%
  ggplot(aes(x = station_type)) +
    geom_col(aes(y = pct_obs)) +
    facet_grid(. ~ variable)
print(p)
dev.off()

report("  Annual share of observations by station type")
file.path(vis_dir, "meteo-annual-obs_per_stype-pct.png") %>% png(., width = 1920, height = 900)
p <-
  smry_by_year_stype %>%
  group_by(year, variable) %>%
  summarize("obs_this_year" = sum(n_obs)) %>%
  left_join(smry_by_year_stype, ., by = c("year", "variable")) %>%
  mutate("pct_obs" = n_obs / obs_this_year) %>%
  ggplot(aes(x = station_type)) +
    geom_col(aes(y = pct_obs)) +
    facet_grid(variable ~ year)
print(p)
dev.off()

report("  Annual cumulative share of observations by station type")
file.path(vis_dir, "meteo-annual-obs_per_stype-cumpct.png") %>% png(., width = 1920, height = 900)
p <-
  smry_by_year_stype %>%
  group_by(year, variable) %>%
  summarize("obs_this_year" = sum(n_obs)) %>%
  left_join(smry_by_year_stype, ., by = c("year", "variable")) %>%
  group_by(year, variable) %>%
  mutate("cum_pct_obs" = cumsum(n_obs / obs_this_year)) %>%
  ggplot(aes(x = station_type)) +
    geom_col(aes(y = cum_pct_obs)) +
    facet_grid(variable ~ year)
print(p)
dev.off()

report("  Annual monthly share of observations by observations per station")
file.path(vis_dir, "meteo-annual-monthly-obs_per_station-pct.png") %>% png(., width = 1920, height = 900)
p <-
  smry_by_year_month_obs_per_station %>%
  ggplot(aes(x = obs_per_station)) +
  geom_col(aes(y = pct_of_obs_this_month)) +
  facet_grid(month ~ year)
print(p)
dev.off()

rm(p)


# Explore data ----------------------------------------------------------------

# Columns of interest
explore_cols <- c("insee_id", "latitude", "longitude", "elevation", "station_type", "year", "month",
                  "date", var_names)

## 1. Outliers (compared to mean for year + month) ----------------------------

# Get the mean and sd of temperature measurements for each year and month
normals <-
  smry_by_year_month %>%
  mutate(
    "normal.mean" = mean,
    "normal.sd" = sd
  ) %>%
  select(year, month, variable, normal.mean, normal.sd)

# Calculate how far each observation is from the normal for the year and month
obs_dists <-
  obs %>%
  select(explore_cols) %>%
  gather(variable, value, var_names) %>%
  left_join(normals, by = c("year", "month", "variable")) %>%
  filter(!is.na(value)) %>%
  mutate("dist" = abs(value - normal.mean) / normal.sd)
rm(normals)
# 9 seconds

# Calculate the average and sd of distance for each station
station_dists <-
  obs_dists %>%
  group_by(insee_id, variable) %>%
  summarize(
    "station_obs" = n(),
    "station.dist" = mean(dist),
    "station.dist.sd" = sd(dist)
  )

# Extract observations that are:
# * Far from the mean for the month and the year
# * Farther than is normal for the station
outliers <-
  obs_dists %>%
  filter(dist >= 3) %>%
  left_join(station_dists, by = c("insee_id", "variable")) %>%
  select(-normal.mean, -normal.sd, -station_obs) %>%
  mutate("adj_dist" = dist - station.dist - (2 * station.dist.sd)) %>%
  filter(adj_dist > 0)

outliers %>% group_by(variable, date) %>% tally %>% spread(variable, n)

# TODO find outliers that don't have other outliers nearby in space and time

# TODO exclude stations that don't record temperature_mean?

# Cleanup ---------------------------------------------------------------------

rm(obs, obs_dists, outliers, smry_by_date, smry_by_doy, smry_by_month, smry_by_station,
   smry_by_stype, smry_by_year_month, smry_by_year_month_obs_per_station, smry_by_year_month_stype,
   smry_by_year_station, smry_by_year_stype, station_dists, explore_cols, ncores, var_names,
   vis_dir, group_and_summarize)

report("Done")
