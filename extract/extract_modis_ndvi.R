# Extract MODIS NDVI data for the reference grid cells
#
# For each satellite (aqua / terra)
# * For each month with data
#   - Load and properly scale the monthly NDVI data
#   - Extract the NDVI and QA data by the reference grid points
#   - Add the MODIS grid id and save

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
source("helpers/modis_ndvi_qa_code_table.R")

report("")
report("Extracting MODIS NDVI data")
report("---")

# Load the reference grid and project it to MODIS sinusoidal
report("Loading 1 km reference grid")
grid <-
  file.path(constants$grid_lst_1km_dir, "grid_lst_1km.rds") %>%
  readRDS %>%
  spTransform(constants$proj_sinu)

# Detect the number of cores available
ncores <- get_ncores()

########################
# MODIS Monthly 1km NDVI
########################

# Load a code table to aid in deciphering MODIS NDVI QA bit codes
report("Loading QA code table")
qa_codes <- modis_ndvi_qa_code_table() %>% as.data.table
# 16 seconds

# Iterate over years to extract the data
# This limits parallelization to 24, but allows progress reporting
for (year in constants$model_years) {
  # Create a directory to hold the extracted data
  year_dir <- file.path(constants$grid_lst_1km_extracts_dir, year)
  dir.create(year_dir, showWarnings = FALSE)
  paste("  Extracting", year, "data to", year_dir) %>% report

  # Find all directories that contain data for the year (for both satellites)
  data_dirs <-
    file.path(constants$data_dir, "modis", constants$modis_satellites) %>%
    list.files(pattern = "^M.D13A3\\.006$", full.names = TRUE) %>%
    list.files(pattern = paste0(year, "-\\d{2}-\\d{2}"), full.names = TRUE)

  # Extract data from all dirs in parallel
  mclapply(data_dirs, function(data_dir) {
    # Create a template to hold the extracted data
    result <- data.table(
      "lst_1km_id" = grid$id,
      "month" = basename(data_dir) %>% as.Date %>% format("%Y-%m"),
      "ndvi" = NA,
      "ndvi_usefulness" = NA
    )
    # 12.6 MB

    # Get all tiles for the date
    tiles <- list.files(data_dir, full.names = TRUE, pattern = "\\.hdf$")

    # Load and properly scale monthly NDVI and QA, merge into a single stack, and crop to the grid.
    # It's ok to merge because MODIS tiles align without overlap.
    datasets <- lapply(tiles, function(tile) {
      sds <- get_subdatasets(tile)

      # Find the monthly NDVI dataset
      ndvi <- grepl("1 km monthly NDVI$", sds) %>% sds[.]

      # Get the scale factor from the dataset metadata
      ndvi_metadata <- gdalinfo(ndvi)
      ndvi_scale_factor <-
        grep("  scale_factor=", ndvi_metadata) %>%
        ndvi_metadata[.] %>%
        gsub("  scale_factor=(.+)", "\\1", .) %>%
        as.numeric

      # Load the NDVI dataset as a raster and apply the scale factor
      # The raster package does this automatically, but for some reason in the metadata the NDVI
      # scale factor is stored as 10000 when it should be 0.0001. So to properly scale NDVI we must
      # divide by the square of the scale factor
      ndvi <- raster(ndvi) / (ndvi_scale_factor^2)

      # Load the QA data, stack with NDVI, and return the stack
      grepl("1 km monthly VI Quality", sds) %>% sds[.] %>% raster %>% stack(ndvi, .)
    }) %>% do.call(raster::merge, .) %>% crop(., grid, snap = "out")
    names(datasets) <- c("ndvi", "qa")

    # Confirm the grid is in the same projection as the data
    if (!identical(projection(datasets), projection(grid))) {
      grid <- spTransform(grid, projection(ndvi))
    }

    # Extract NDVI and QA by the reference grid points
    # TODO test extracting data by buffers -> slower but more accurate, is it worth the effort?
    datasets <- raster::extract(datasets, grid, df = TRUE)

    # Add NDVI to the result template
    result[, ndvi := datasets$ndvi]

    # Look up the usefulness from the QA codes table and add to the template
    u <- data.table("code" = datasets$qa) %>% qa_codes[., usefulness, on = "code"]
    result[, ndvi_usefulness := u]
    rm(u)

    # Key on LST 1 km id
    result %>% setkey("lst_1km_id")

    # Save the extracted data
    satellite <- data_dir %>% dirname %>% dirname %>% basename
    path <-
      basename(data_dir) %>%
      format.Date("%Y-%m") %>%
      paste0("grid_lst_1km_", satellite, "_ndvi_", ., ".rds") %>%
      file.path(year_dir, .)
    saveRDS(result, path)

    # Return nothing to save memory
    rm(result)
  }, mc.cores = ncores, mc.preschedule = FALSE)
}

report("Done")
