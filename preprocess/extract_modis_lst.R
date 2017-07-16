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

# Extract Aqua and Terra data in sequence
for(satellite in c("aqua", "terra")) {
  paste("Processing MODIS", satellite, "LST data") %>% report

  # Set the directory that contains the LST data for the satellite
  lst_dir <-
    file.path(modis_dir, satellite) %>%
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
    year_dir <- format(month, "%Y") %>% file.path(extracts_dir, .)
    dir.create(year_dir, showWarnings = FALSE)

    # List the LST data directories corresponding to each date in the month
    # Note that some of these directories may not exist
    date_dirs <-
      (seq.Date(month, length.out = 2, by = "month")[2] - 1) %>%
      seq.Date(month, ., by = 1) %>%
      file.path(lst_dir, .)

    # Extract day and night LST for each date in the month
    # Note that some dates may not have data
    lapply(c("Day", "Night"), function(tod) {
      format(month, "%Y-%m") %>%
        paste("  Extracting", ., satellite, tod, "LST to", year_dir) %>%
        report

      # Extract data for all dates in the month in parallel
      result <- mclapply(date_dirs, function(date_dir) {
        # Create a template for the extracted data
        extracted <- data.frame(
          "modis_grid_id" = grid$id,
          "date" = basename(date_dir),
          "lst" = NA,
          "lst_error" = NA,
          stringsAsFactors = FALSE
        )
        # 12.6 MB

        # Extract any data for the date to the template
        if (file.exists(date_dir)) {
          # Get all tiles for the date
          tiles <- list.files(date_dir, full.names = TRUE, pattern = "\\.hdf$")

          # Load the LST data from the tiles, merge into a single raster, and
          # crop to the reference grid. It's ok to merge rather than mosaic
          # because MODIS tiles align without overlap.
          lst <- lapply(tiles, function(tile) {
            get_subdatasets(tile) %>%
            .[grep(paste("LST", tod, "1km$", sep = "_"), .)] %>%
            raster
          }) %>% do.call(merge, .) %>% crop(., grid, snap = "out")
          # 5 seconds
          # 9.03 MB

          # Extract the LST data by the reference grid points
          # TODO test extracting data by buffers -> slower but more accurate, is
          # the extra accuracy worth the effort?
          extracted$lst <- extract(lst, grid)
          # 2 seconds

          # Load, merge, and crop the QA data
          qa <- lapply(tiles, function(tile) {
            get_subdatasets(tile) %>%
            .[grepl(paste("QC", tod, sep = "_"), .)] %>%
            raster
          }) %>% do.call(merge, .) %>% crop(., grid, snap = "out")
          # 4 seconds
          # 4.52 MB

          # Extract the QA data by the reference grid points and look up the
          # corresponding LST error from the QA codes table
          extracted$lst_error <-
            extract(qa, grid) %>%
            data.frame("code" = .) %>%
            left_join(., qa_codes[c("code", "lst_error")], by = "code") %>%
            .$lst_error
          # 2 seconds
        }
        # 12 seconds
        # 17.6 MB

        # Return the template (which may have no data)
        extracted
      }, mc.cores = ncores, mc.preschedule = FALSE) %>% do.call(rbind, .)
      # 70 seconds (45 for mclapply, 25 for rbind)
      # 527 MB

      # Save the extracted data in fst file format with 90% compression
      # write.fst is 3x faster than saveRDS
      output_path <-
        format(month, "%Y-%m") %>%
        paste0("modis_grid_", satellite, "_lst_", tod, "_", ., ".fst") %>%
        tolower(.) %>%
        file.path(year_dir, .)
      write.fst(result, output_path, compress = 90)
      # 12 seconds

      # Clear memory
      rm(result)
    })
    # 170 seconds (85 seconds each day / night)
  })
}

report("Done")
