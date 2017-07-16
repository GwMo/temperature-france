# Extract MODIS NDVI data for the reference grid cells
#
# For each satellite (aqua / terra)
# * For each month with data
#   - Load and properly scale the monthly NDVI data
#   - Extract the NDVI and QA data by the reference grid points
#   - Add the MODIS grid id and save

library(magrittr)  # %>% pipe-like operator
library(parallel)  # parallel computation
library(dplyr)     # data manipulation e.g. joining tables
library(sp)        # classes and methods for spatial data
library(rgdal)     # wrapper for GDAL and proj.4 to manipulate spatial data
library(raster)    # methods to manipulate gridded spatial data
library(gdalUtils) # extends rgdal and raster to manipulate HDF4 files

# Set directories and load helper functions
file.path("~", "temperature-france", "helpers", "set_dirs.R") %>% source
file.path(helpers_dir, "report.R") %>% source
file.path(helpers_dir, "get_ncores.R") %>% source


report("Extracting MODIS NDVI data")


# Load the reference grid
report("Loading MODIS reference grid")
grid <- file.path(grids_dir, "modis_grid.rds") %>% readRDS

# Detect the number of cores available
ncores <- get_ncores()

########################
# MODIS Monthly 1km NDVI
########################

modis_dir <- file.path(data_dir, "modis")

# Project the grid to match the MODIS data
report("Projecting grid to MODIS sinusoidal")
grid <- spTransform(grid, "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs")

# Load a code table to aid in deciphering MODIS NDVI QA bit codes
file.path(helpers_dir, "modis_ndvi_qa_code_table.R") %>% source
qa_codes <- modis_ndvi_qa_code_table()
# 16 seconds

# Extract Aqua and Terra data in sequence
for(satellite in c("aqua", "terra")) {
  paste("Processing MODIS", satellite, "NDVI data") %>% report

  # Set the directory that contains the NDVI data for the satellite
  ndvi_dir <-
    file.path(modis_dir, satellite) %>%
    list.files(., pattern = "^M.D13A3\\.006$", full.names = TRUE)

  # Extract the NDVI data for all months in parallel
  date_dirs <- list.dirs(ndvi_dir, recursive = FALSE)
  mclapply(date_dirs, function(date_dir) {
    # Parse the date from the directory name
    d <- basename(date_dir) %>% as.Date

    # Create a directory to hold the extracted data
    year_dir <- format(d, "%Y") %>% file.path(extracts_dir, .)
    dir.create(year_dir, showWarnings = FALSE)

    format(d, "%Y-%m") %>%
      paste("  Extracting", ., satellite, "monthly NDVI to", year_dir) %>%
      report

    # Create a template to hold the extracted data
    result <- data.frame(
      "modis_grid_id" = grid$id,
      "date" = basename(date_dir),
      "ndvi" = NA,
      "ndvi_usefulness" = NA,
      stringsAsFactors = FALSE
    )
    # 12.6 MB

    # Get all tiles for the date
    tiles <- list.files(date_dir, full.names = TRUE, pattern = "\\.hdf$")

    # Load and properly scale monthly NDVI, merge into a single raster, and crop
    # to the reference grid. It's ok to merge rather than mosaic because MODIS
    # tiles align without overlap.
    ndvi <- lapply(tiles, function(tile) {
      # Find the monthly NDVI dataset
      datasets <- get_subdatasets(tile)
      ndvi_dataset <- grepl("1 km monthly NDVI$", datasets) %>% datasets[.]

      # Get the scale factor from the dataset metadata
      ndvi_metadata <- gdalinfo(ndvi_dataset)
      ndvi_scale_factor <-
        grep("  scale_factor=", ndvi_metadata) %>%
        ndvi_metadata[.] %>%
        gsub("  scale_factor=(.+)", "\\1", .) %>%
        as.numeric

      # Load the dataset as a raster and apply the scale factor
      # NOTE: the scale factor is applied automatically when loading the raster
      # but for some reason it is stored in the metadata as a divisor rather
      # than a multiplier (i.e. 10000 rather than 0.0001). Thus to properly
      # scale the data we need to divide by the square of the scale factor.
      raster(ndvi_dataset) / (ndvi_scale_factor^2)
    }) %>% do.call(merge, .) %>% crop(., grid, snap = "out")
    # 10 seconds
    # 9.03 MB

    # Extract the NDVI data by the reference grid points
    # TODO test extracting data by buffers -> slower but more accurate, is the
    # extra accuracy worth the effort?
    result$ndvi <- extract(ndvi, grid)
    # 2 seconds
    # 15.1 MB

    # Load, merge, and crop monthly NDVI QA data
    qa <- lapply(tiles, function(tile) {
      get_subdatasets(tile) %>%
      .[grepl("1 km monthly VI Quality", .)] %>%
      raster
    }) %>% do.call(merge, .) %>% crop(., grid, snap = "out")
    # 6 seconds
    # 4.52 MB

    # Extract the QA data by the reference grid points and look up the
    # corresponding usefulness from the QA codes table
    result$ndvi_usefulness <-
      extract(qa, grid) %>%
      data.frame("code" = .) %>%
      left_join(., qa_codes[c("code", "usefulness")], by = "code") %>%
      .$usefulness
    # 2 seconds
    # 17.6 MB

    # Save the extracted data
    output_path <-
      format(d, "%Y-%m") %>%
      paste0("modis_grid_", satellite, "_ndvi_", ., ".rds") %>%
      file.path(year_dir, .)
    saveRDS(result, output_path)

    # Return NULL to save memory
    NULL
  }, mc.cores = ncores, mc.preschedule = FALSE)
}

report("Done")
