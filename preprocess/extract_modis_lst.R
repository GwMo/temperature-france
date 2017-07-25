# Extract MODIS land surface temperature data for the reference grid cells
#
# For each satellite (aqua / terra)
# * For each month from the first month with data to December 2016
#   - For each time of day (day / night)
#     + For each date in the month
#         Create a template with MODIS grid id and date to hold extracted data
#         Extract LST and QA info to the template if data exists for the date
#     + Combine the templates for all dates in the month and save

library(magrittr)  # %>% pipe-like operator
library(parallel)  # parallel computation
library(dplyr)     # data manipulation e.g. joining tables
library(fst)       # fast serialization
library(sp)        # classes and methods for spatial data
library(rgdal)     # wrapper for GDAL and proj.4 to manipulate spatial data
library(raster)    # methods to manipulate gridded spatial data
library(gdalUtils) # extends rgdal and raster to manipulate HDF4 files

# Set directories and load helper functions
file.path("~", "temperature-france", "helpers", "set_dirs.R") %>% source
file.path(helpers_dir, "report.R") %>% source
file.path(helpers_dir, "get_ncores.R") %>% source


report("Extracting MODIS land surface temperature data")


# Load the reference grid
report("Loading MODIS reference grid")
grid <- file.path(grids_dir, "modis_grid.rds") %>% readRDS

# Detect the number of cores available
ncores <- get_ncores()

#####################
# MODIS daily 1km LST
#####################

modis_dir <- file.path(data_dir, "modis")

# Project the grid to match the MODIS data
report("Projecting grid to MODIS sinusoidal")
grid <- spTransform(grid, "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs")

# Load a code table to aid in deciphering MODIS LST QA bit codes
file.path(helpers_dir, "modis_lst_qa_code_table.R") %>% source
qa_codes <- modis_lst_qa_code_table()

# List the scientific datasets we want to extract
to_extract <- c(
  "LST_Day_1km",
  "LST_Night_1km",
  "QC_Day",
  "QC_Night",
  "Emis_31",
  "Emis_32"
)

# Extract Aqua and Terra data in sequence
for(sat in c("aqua", "terra")) {
  paste("Processing MODIS", sat, "LST data") %>% report

  # Set the directory that contains the LST data for the satellite
  lst_dir <-
    file.path(modis_dir, sat) %>%
    list.files(., pattern = "^M.D11A1\\.006$", full.names = TRUE)

  # List all months from the first month with data to December 2016
  months <-
    list.files(lst_dir, pattern = "^\\d{4}-\\d{2}-\\d{2}$")[1] %>%
    gsub("(\\d{4}-\\d{2}).+", "\\1-01", .) %>%
    as.Date %>%
    seq.Date(., as.Date("2016-12-01"), by = "month")

  # Iterate over the months
  lapply(months, function(month) {
    # Create a directory to hold the extracted data
    year_dir <- format(month, "%Y") %>% file.path(extracts_dir, "modis", .)
    dir.create(year_dir, showWarnings = FALSE)

    # List the LST data directories corresponding to each date in the month
    # Note that some of these directories may not exist
    date_dirs <-
      (seq.Date(month, length.out = 2, by = "month")[2] - 1) %>%
      seq.Date(month, ., by = 1) %>%
      file.path(lst_dir, .)

    # Extract day and night LST, emissivity, and QA for each date in the month
    # Note that some dates may not have data
    paste("  Extracting", format(month, "%Y-%m"), sat, "LST to", year_dir) %>% report
    result <- mclapply(date_dirs, function(date_dir) {
      # Create a template for the extracted data
      template <- data.frame(
        "modis_grid_id" = grid$id,
        "date" = basename(date_dir),
        "day_lst" = NA,
        "day_lst_error" = NA,
        "night_lst" = NA,
        "night_lst_error" = NA,
        "emissivity" = NA,
        "day_emis_error" = NA,
        "night_emis_error" = NA,
        stringsAsFactors = FALSE
      )
      # 25.1 MB

      # Extract any data for the date to the template
      if (file.exists(date_dir)) {
        # Get all tiles for the date
        tiles <- list.files(date_dir, full.names = TRUE, pattern = "\\.hdf$")

        # Load all datasets of interest from the tiles, merge into a single
        # stack, and crop to the reference grid. It's ok to merge rather than
        # mosaic because MODIS tiles align without overlap.
        datasets <- lapply(tiles, function(tile) {
          sds <- get_subdatasets(tile)
          sds_names <- sapply(strsplit(sds, ":"), last)
          stacked <- sds[sds_names %in% to_extract] %>% stack
          names(stacked) <- sds_names[sds_names %in% to_extract]
          stacked
        })
        layer_names <- names(datasets[[1]])
        datasets <- do.call(merge, datasets) %>% crop(., grid, snap = "out")
        names(datasets) <- layer_names
        # 16 seconds
        # 54.1 MB

        # Calculate mean of bands 31 and 32 emissivity
        datasets$emissivity <- mean(datasets$Emis_31, datasets$Emis_32)
        datasets <- dropLayer(datasets, c("Emis_31", "Emis_32"))
        # 45.1 MB

        # Extract the data by the reference grid points
        # TODO test disaggregating data and extracting by buffers -> slower but
        # more accurate, is the extra accuracy worth the effort?
        datasets <- extract(datasets, grid, df = TRUE)
        # 7 seconds
        # 30.1 MB

        # Add LST and emissivity to the template
        template$day_lst    <- datasets$LST_Day_1km
        template$night_lst  <- datasets$LST_Night_1km
        template$emissivity <- datasets$emissivity
        # 32.6 MB

        # Look up the day LST and emissivity error from the QA codes
        template[c("day_lst_error", "day_emis_error")] <- left_join(
          data.frame("code" = datasets$QC_Day),
          qa_codes[c("code", "emis_error", "lst_error")],
          by = "code"
        )[c("lst_error", "emis_error")]
        # 37.7 MB

        # Look up the night LST and emissivity error from the night QA codes
        template[c("night_lst_error", "night_emis_error")] <- left_join(
          data.frame("code" = datasets$QC_Night),
          qa_codes[c("code", "emis_error", "lst_error")],
          by = "code"
        )[c("lst_error", "emis_error")]
        # 42.7 MB
      }
      # 27 seconds
      # 42.7 MB

      # Return the template (which may have no data)
      template
    }, mc.cores = ncores) %>% do.call(rbind, .)
    # 136 seconds (80 for mclapply, 56 for rbind)
    # 1.28 GB

    # Save the extracted data in fst file format with 75% compression
    # write.fst is 3x faster than saveRDS
    output_path <-
      format(month, "%Y-%m") %>%
      paste0("modis_grid_", sat, "_lst_", ., ".fst") %>%
      file.path(year_dir, .)
    write.fst(result, output_path, compress = 75)
    # 24 seconds

    # Clear memory
    rm(result)
  })
  # 165 seconds per month
}

report("Done")
