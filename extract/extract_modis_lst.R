# Extract MODIS land surface temperature data for the reference grid cells
#
# For each satellite (aqua / terra)
# * For each month from the first month with data to December 2016
#   - For each time of day (day / night)
#     + For each date in the month
#         Create a template with MODIS grid id and date to hold extracted data
#         Extract LST and QA info to the template if data exists for the date
#     + Combine the templates for all dates in the month and save

library(magrittr)   # %>% pipe-like operator
library(parallel)   # parallel computation
library(data.table) # fast data manipulation
library(sp)         # classes and methods for spatial data
library(rgdal)      # wrapper for GDAL and proj.4 to manipulate spatial data
library(raster)     # methods to manipulate gridded spatial data
library(gdalUtils)  # extends rgdal and raster to manipulate HDF4 files

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")
source("helpers/modis_lst_qa_code_table.R")

report("")
report("Extracting MODIS land surface temperature data")
report("---")

# Load the reference grid and project it to MODIS sinusoidal
report("Loading 1 km reference grid")
grid <-
  file.path(constants$grid_lst_1km_dir, "grid_lst_1km.rds") %>%
  readRDS %>%
  spTransform(constants$proj_sinu)

# Detect the number of cores available
ncores <- get_ncores()

# Load a code table to aid in deciphering MODIS LST QA bit codess
qa_codes <- modis_lst_qa_code_table() %>% as.data.table

# List the scientific datasets we want to extract
to_extract <- c(
  "LST_Day_1km",
  "LST_Night_1km",
  "QC_Day",
  "QC_Night",
  "Emis_31",
  "Emis_32"
)

# List the first day of each month in the model timeperiod
month_starts <- seq.Date(
  from = constants$model_years %>% first %>% paste0("-01-01") %>% as.Date,
  to = constants$model_years %>% last %>% paste0("-12-01") %>% as.Date,
  by = "month"
) %>% format("%Y-%m-%d") # Format as string b/c `for` will convert a date to an integer

# Extract data by month
for (month_start in month_starts) {
  month_start <- as.Date(month_start) # `for` converts dates to integers
  format(month_start, "%Y-%m") %>% report

  # Create a directory to hold the extracted data
  year_dir <- format(month_start, "%Y") %>% file.path(constants$grid_lst_1km_extracts_dir, .)
  dir.create(year_dir, showWarnings = FALSE)

  # Extract Aqua and Terra data in sequence
  for(sat in constants$modis_satellites) {
    # Set the directory that contains the LST data for the satellite
    lst_dir <-
      file.path(constants$data_dir, "modis", sat) %>%
      list.files(., pattern = "^M.D11A1\\.006$", full.names = TRUE)

    # List the LST data directories corresponding to all dates in the month
    # Some of these directories may not exist, as some dates have no data
    data_dirs <-
      seq.Date(from = month_start, length.out = 2, by = "month") %>%
      last %>%
      subtract(1) %>%
      seq.Date(from = month_start, to = ., by = 1) %>%
      file.path(lst_dir, .)

    if (!any(file.exists(data_dirs))) {
      paste("  Skipping", sat, "LST (no data)") %>% report
    } else {
      # Extract day and night LST, emissivity, and QA for each date in the month
      # Some of these dates may have no data - in that case, return a template filled with NA
      paste("  Extracting", sat, "LST") %>% report
      result <- mclapply(data_dirs, function(data_dir) {
        # Create a template for the extracted data
        template <- data.table(
          "date" = basename(data_dir),
          "lst_1km_id" = grid$id,
          "day_lst" = NA,
          "day_lst_error" = NA,
          "night_lst" = NA,
          "night_lst_error" = NA,
          "emissivity" = NA,
          "day_emis_error" = NA,
          "night_emis_error" = NA
        )
        # 72.8 MB

        # Extract any data for the date to the template
        if (file.exists(data_dir)) {
          # Get all tiles for the date
          tiles <- list.files(data_dir, full.names = TRUE, pattern = "\\.hdf$")

          # Load all datasets of interest from the tiles, merge into a single stack, and crop to the
          # LST 1 km grid. It's ok to merge because MODIS tiles align without overlap.
          datasets <- lapply(tiles, function(tile) {
            sds <- get_subdatasets(tile)
            sds_names <- sapply(strsplit(sds, ":"), last)
            stacked <- sds[sds_names %in% to_extract] %>% stack
            names(stacked) <- sds_names[sds_names %in% to_extract]
            stacked
          })
          layer_names <- names(datasets[[1]])
          datasets <- do.call(raster::merge, datasets) %>% crop(., grid, snap = "out")
          names(datasets) <- layer_names
          # 16 seconds
          # 54.1 MB

          # Calculate mean of bands 31 and 32 emissivity
          datasets$emissivity <- mean(datasets$Emis_31, datasets$Emis_32)
          datasets <- dropLayer(datasets, c("Emis_31", "Emis_32"))
          # 45.1 MB

          # Confirm the grid is in the same projection as the data
          if (!identical(projection(datasets), projection(grid))) {
            grid <- spTransform(grid, projection(ndvi))
          }

          # Extract the data by the reference grid points
          # TODO test disaggregating data and extracting by buffers -> slower but
          # more accurate, is the extra accuracy worth the effort?
          datasets <- raster::extract(datasets, grid, df = TRUE)
          # 7 seconds
          # 30.1 MB

          # Add LST and emissivity to the template
          template[, day_lst := datasets$LST_Day_1km]
          template[, night_lst := datasets$LST_Night_1km]
          template[, emissivity := datasets$emissivity]
          # 80.4 MB

          # Look up the day LST and emissivity error from the QA codes
          errors <-
            data.table("code" = datasets$QC_Day) %>%
            qa_codes[., c("lst_error", "emis_error"), on = "code", with = FALSE]
          template[, c("day_lst_error", "day_emis_error") := errors]
          # 85.4 MB

          errors <-
            data.table("code" = datasets$QC_Night) %>%
            qa_codes[., c("lst_error", "emis_error"), on = "code", with = FALSE]
          template[, c("night_lst_error", "night_emis_error") := errors]
          # 90.4 MB
        }
        # 40 seconds
        # 90.4 MB

        # Return the template (which may have no data)
        template
      }, mc.cores = ncores) %>% rbindlist
      # 60 seconds
      # 1.45 GB

      # Key on date then LST 1 km id
      setkeyv(result, c("date", "lst_1km_id"))

      # Save the extracted data
      path <-
        format(month_start, "%Y-%m") %>%
        paste0("grid_lst_1km_", sat, "_lst_", ., ".rds") %>%
        file.path(year_dir, .)
      paste("    Saving to", path) %>% report
      saveRDS(result, path)
      # 60 seconds, 135 MB for RDS
      # 40 seconds, 210 MB for fst compress = 100

      # Clear memory
      rm(result)
    }
  }
  # 120 seconds per satellite
}
# 240 seconds per year

report("Done")
