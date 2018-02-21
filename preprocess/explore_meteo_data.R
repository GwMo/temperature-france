# Explore the Meteo France station data ---------------------------------------

library(magrittr)   # %>% pipe-like operator
library(parallel)   # parallel computation
library(sp)         # classes and methods for spatial data
library(rgdal)      # wrapper for GDAL and proj.4 to translate spatial data
library(rgeos)      # wrapper for GEOS to manipulate vector data
library(dplyr)      # data manipulation e.g. joining tables
library(tidyr)      # data tidying
library(ggplot2)    # plotting
library(data.table) # fast data manipulation

# library(mapview)  # interactive maps
# library(cowplot)  # aligning multiple plots
# library(plotly)   # interactive plots

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")

report("")
report("Exploring clean Meteo France station data")
report("---")

# Setup -----------------------------------------------------------------------

# Number of cores to use for parallel computation
ncores <- length(constants$model_years) + 1 %>% min(., get_ncores())

# Create a directory to hold visualizations
vis_dir <- file.path(constants$work_dir, "meteo_france", "observations", "vis")
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

paste(
  "Loading observations for",
  dplyr::first(constants$model_years),
  "to",
  dplyr::last(constants$model_years)
) %>% report
clean_dir <- file.path(constants$work_dir, "meteo_france", "observations", "clean")
obs <- mclapply(constants$model_years, function(year) {
  x <- paste0("meteo_data_", year, "-clean.rds") %>% file.path(clean_dir, .) %>% readRDS
  x[, date := as.Date(date)]
}, mc.cores = ncores) %>% rbindlist
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

# Assemble the data
lhs <-
  smry_by_date %>%
  dplyr::select(variable, date, min, mean, max) %>%
  mutate(
    "year" = format.Date(date, "%Y") %>% as.integer,
    "month" = format.Date(date, "%m") %>% as.integer
  )
join_cols <- c("variable", "year", "month")
normals_cols <- c("mean", "sd", "median", "iqr")
rhs <-
  smry_by_year_month %>%
  dplyr::select(c(join_cols, normals_cols))
names(rhs) <- paste0("normal.", normals_cols) %>% c(join_cols, .)
plot_data <- left_join(lhs, rhs, by = join_cols)

# Plot the data
for (v in var_names) {
  paste0("meteo-daily-", v, ".png") %>%
    file.path(vis_dir, .) %>%
    png(., width = 11520, height = 900)
  p <-
    plot_data %>%
    filter(variable == v) %>%
    ggplot(aes(x = date)) +
      geom_ribbon(aes(
        ymin = normal.mean + (2 * normal.sd),
        ymax = normal.mean - (2 * normal.sd)
      ), alpha = 0.5, fill = "grey") +
      geom_line(aes(y = min),  colour = "blue") +
      geom_line(aes(y = mean), colour = "black") +
      geom_line(aes(y = max),  colour = "red") +
      geom_line(aes(y = normal.median + (1.5 * normal.iqr)),  colour = "red",  linetype = "dashed") +
      geom_line(aes(y = normal.median - (1.5 * normal.iqr)),  colour = "blue", linetype = "dashed") +
      scale_x_date(
        breaks = seq.Date(min(obs$date), max(obs$date), by = "month"),
        expand = c(0, 0),
        name = element_blank()
      ) +
      scale_y_continuous(breaks = seq(-40, 50, 10), name = "°C")
  print(p)
  dev.off()
}
rm(lhs, plot_data, rhs, join_cols, normals_cols, v)

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
        from = min(constants$model_years) %>% paste0("-01-01") %>% as.Date,
        to =   max(constants$model_years) %>% paste0("-12-31") %>% as.Date,
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
explore_cols <- c("insee_id", "latitude", "longitude", "station_elevation", "station_type", "year",
                  "month", "doy", "date", var_names)

## 1. Co-located observations

obs_t <- as.data.table(obs)

# Select observations from stations that have the same location as another station
colocated <-
  obs_t[, .N, by = c("date", "latitude", "longitude")] %>%
  .[N > 1, ] %>%
  obs_t[., on = c("date", "latitude", "longitude")]

# For each location and date, identify the lowest station type (should be highest quality)
colocated[
  ,
  "best_station_type" := levels(.SD$station_type)[min(as.integer(.SD$station_type))],
  by = c("date", "latitude", "longitude")
]

# Calculate the difference between the observations for each location date
diffs <-
  colocated[
    ,
    list(
      "n" = .N,
      "types" = paste(.SD$station_type, collapse = ","),
      "elev_diff" = max(.SD$station_elevation) - min(.SD$station_elevation),
      "tmin_diff"  = max(.SD$temperature_min, na.rm = TRUE) - min(.SD$temperature_min,  na.rm = TRUE),
      "tmean_diff"  = max(.SD$temperature_mean, na.rm = TRUE) - min(.SD$temperature_mean,  na.rm = TRUE),
      "tmax_diff"  = max(.SD$temperature_max, na.rm = TRUE) - min(.SD$temperature_max,  na.rm = TRUE)
    ),
    by = c("date", "latitude", "longitude")
  ]

# For each location, get the maximum discrepancy between observations
max_diffs <-
  diffs[
    ,
    lapply(.SD, max),
    by = c("latitude", "longitude"),
    .SDcols = c("elev_diff", "tmin_diff", "tmean_diff", "tmax_diff")
  ]

# For each location, get the 95% percentile of the discrepancy between observations
percentiles <-
  diffs[
    ,
    lapply(.SD, quantile, probs = 0.95),
    by = c("latitude", "longitude"),
    .SDcols = c("elev_diff", "tmin_diff", "tmean_diff", "tmax_diff")
  ]

# Map the max discrepancies
pts <- SpatialPointsDataFrame(
  coords = max_diffs[, c("longitude", "latitude"), with = FALSE],
  data = max_diffs,
  proj4string = CRS("+proj=longlat +datum=WGS84 +no_defs")
)
mapview(pts)

splits <- split(diffs, diffs[, paste(round(latitude, 5), round(longitude, 5), sep = "-")])
hists <- lapply(splits, function(splt) {
  list(
    "tmin_diff" = splt[, tmin_diff] %>% hist(plot = FALSE),
    "tmax_diff" = splt[, tmax_diff] %>% hist(plot = FALSE)
  )
})


## 2. Outliers (compared to mean for year + month) ----------------------------

# Get normal temperatures for each month and year
normals <-
  smry_by_year_month %>%
  mutate(
    "normal.median" = median,
    "normal.iqr" = iqr
  ) %>%
  dplyr::select(year, month, variable, normal.median, normal.iqr)

# Calculate how many IQRs each observation is from the normal
obs_dists <-
  obs %>%
  dplyr::select(explore_cols) %>%
  gather(variable, value, var_names) %>%
  left_join(normals, by = c("year", "month", "variable")) %>%
  filter(!is.na(value)) %>%
  mutate("dist" = (value - normal.median) / normal.iqr)
rm(normals)
# 9 seconds

# Select observations that are more than 1.5 IQR from the normal
outliers <-
  obs_dists %>%
  filter(abs(dist) > 1.5) %>%
  mutate(
    "split_group" = if_else(dist > 0, "> normal", "< normal") %>% paste(date, variable, .)
  )

# Examples
#   Unusually high tmax in Brittany = likely problem
#   outliers %>% filter(date == "2010-11-03" & variable == "temperature_max")
#
#   Unusually high tmin in Pyrenees = likely ok
#   outliers %>% filter(date == "2007-02-16" & variable == "temperature_min")

# Make spatial points for the outliers
epsg_4326 <- CRS("+proj=longlat +datum=WGS84 +no_defs")
epsg_3035 <- CRS("+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs")
outlier_pts <-
  SpatialPointsDataFrame(
    coords = dplyr::select(outliers, longitude, latitude),
    data = dplyr::select(outliers, insee_id, station_elevation, date, variable, dist),
    proj4string = epsg_4326
  ) %>% spTransform(epsg_3035)

# Make buffers for extreme outliers
buffers <-
  outlier_pts[abs(outlier_pts$dist) > 3, ] %>%
  gBuffer(byid = TRUE, width = 60000)

# # Explore buffers one by one in an interactive map
# for (i in 1:1) {
#   buf <- buffers[i, ]
#   pts <- outlier_pts[outlier_pts$date == buf$date & outlier_pts$variable == buf$variable, ]
#   map <-
#     pts[!is.na(over(pts, buf)$insee_id), ] %>%
#     mapview(zcol = "dist", at = c(-10, -3, -2, 0, 2, 3, 10), legend = TRUE)
#   print(map)
# }
# rm(buf, map, pts, i)

report("Done")
