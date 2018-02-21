# Check for misalignment between the different MODIS products

library(magrittr)  # %>% pipe-like operator
library(sp)        # classes and methods for spatial data
library(rgdal)     # wrapper for GDAL and proj.4 to manipulate spatial data
library(raster)    # methods to manipulate gridded spatial data
library(gdalUtils) # extends rgdal and raster to manipulate HDF4 files

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")

report("")
report("Checking alignement of MODIS products")
report("---")

# Function to return all files matching a pattern for the first date of
# a MODIS product
list_tiles <- function(path, regexp) {
  dir(path, full.names = TRUE)[1] %>%
  list.files(., full.names = TRUE, pattern = regexp)
}

# Function to compare the alignment of two rasters
check_alignment <- function(a, b) {
  d <- c(
    max(origin(a) - origin(b)),
    max(res(a) - res(b)),
    max(coordinates(a) - coordinates(b))
  )
  names(d) <- c("origin", "resolution", "coordinates")
  d
}

# MODIS products to check
products <- list(
  "aqua_lst"   = file.path(constants$data_dir, "modis", "aqua", "MYD11A1.006"),
  "aqua_ndvi"  = file.path(constants$data_dir, "modis", "aqua", "MYD13A3.006"),
  "terra_lst"  = file.path(constants$data_dir, "modis", "terra", "MOD11A1.006"),
  "terra_ndvi" = file.path(constants$data_dir, "modis", "terra", "MOD13A3.006")
)

# Get the location of the first Aqua LST tile
tile_id <-
  list_tiles(products$aqua_lst, "\\.hdf$")[1] %>%
  regmatches(., regexpr("h\\d{2}v\\d{2}", .))

# Load the the first dataset of the corresponding tile for each MODIS product
rasters <- sapply(products, function(path) {
  paste0(tile_id, ".+\\.hdf$") %>%
  list_tiles(path, .) %>%
  get_subdatasets %>%
  .[1] %>%
  raster
})
rm(tile_id, products)

# Compare alignment
results <- rbind(
  "Aqua LST  - Terra LST"  = check_alignment(rasters$aqua_lst, rasters$terra_lst),
  "Aqua NDVI - Terra NDVI" = check_alignment(rasters$aqua_ndvi, rasters$terra_ndvi),
  "Aqua LST  - Aqua  NDVI" = check_alignment(rasters$aqua_lst, rasters$aqua_ndvi),
  "Terra LST - Terra NDVI" = check_alignment(rasters$terra_lst, rasters$terra_ndvi)
) %>% as.data.frame
print(results)

# Confirm the rasters are aligned to within 1 mm
misalignment <- max(results$coordinates)
if (misalignment < 0.001) {
  report("MODIS products are aligned to within 1 mm")
} else {
  stop(paste("MODIS products are misaligned by", round(misalignment, 2), "m"))
}

report("Done")
