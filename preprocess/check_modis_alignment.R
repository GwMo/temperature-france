# Check for misalignment between the different MODIS products

library(magrittr)  # %>% pipe-like operator
library(sp)        # classes and methods for spatial data
library(rgdal)     # wrapper for GDAL and proj.4 to manipulate spatial data
                     # (GDAL and proj.4 must be present)
library(raster)    # methods to manipulate gridded spatial data
library(gdalUtils) # extends rgdal and raster to manipulate HDF4 files
                     # (GDAL must have been built with HDF4 support)

print("Checking alignement of MODIS products")

# Set directories
modis_dir <- file.path("~", "data", "modis") %>% path.expand
setwd(modis_dir)

# Define a function to return all files matching a pattern for the first date of
# a MODIS product
list_tiles <- function(path, regexp) {
  dir(path, full.names = TRUE)[1] %>%
  list.files(., full.names = TRUE, pattern = regexp)
}

# Define a function to return the maximum difference between the origin,
# resolution, and coordinates of two rasters
difference <- function(a, b) {
  d <- c(
    max(origin(a) - origin(b)),
    max(res(a) - res(b)),
    max(coordinates(a) - coordinates(b))
  )
  names(d) <- c("origin", "resolution", "coordinates")
  d
}

# Set the paths to the MODIS products
products <- list(
  "aqua_lst" = file.path(modis_dir, "aqua", "MYD11A1.006"),
  "aqua_ndvi" = file.path(modis_dir, "aqua", "MYD13A3.006"),
  "terra_lst" = file.path(modis_dir, "terra", "MOD11A1.006"),
  "terra_ndvi" = file.path(modis_dir, "terra", "MOD13A3.006")
)

# Get the location of the first Aqua LST tile
location <-
  list_tiles(products$aqua_lst, "\\.hdf$")[1] %>%
  regmatches(., regexpr("h\\d{2}v\\d{2}", .))

# Load the the first dataset of the tile at that location for each MODIS product
rasters <- sapply(products, function(path) {
  paste0(location, ".+\\.hdf$") %>%
  list_tiles(path, .) %>%
  get_subdatasets(.) %>%
  .[1] %>%
  raster
})

# Calculate misalignment between Aqua and Terra LST rasters
print("Aqua LST vs Terra LST")
lst_vs_lst <- difference(rasters$aqua_lst, rasters$terra_lst)
print(lst_vs_lst)

# Calculate misalignment between Aqua and Terra NDVI rasters
print("Aqua NDVI vs Tera NDVI")
ndvi_vs_ndvi <- difference(rasters$aqua_ndvi, rasters$terra_ndvi)
print(ndvi_vs_ndvi)

# Calculate misalignment between Aqua LST and NDVI rasters
print("Aqua LST vs Aqua NDVI")
aqua_lst_vs_ndvi <- difference(rasters$aqua_lst, rasters$aqua_ndvi)
print(aqua_lst_vs_ndvi)

# Calculate misalignment between Terra LST and NDVI rasters
print("Terra LST vs Terra NDVI")
terra_lst_vs_ndvi <- difference(rasters$terra_lst, rasters$terra_ndvi)
print(terra_lst_vs_ndvi)

# Combine the differences into a data frame
diffs <-
  rbind(lst_vs_lst, ndvi_vs_ndvi, aqua_lst_vs_ndvi, terra_lst_vs_ndvi) %>%
  as.data.frame

# If the rasters are aligned to within 1 mm, use the aqua lst grid as the reference
if (max(diffs$coordinates) < 0.001) {
  print("MODIS products are aligned to within 1 mm")
  print("Proceed to creating a reference grid based on the MODIS LST data")
} else {
  paste("MODIS products are misaligned by up to", max(diffs$coordinates), "m") %>% print
  print("USE CAUTION when extracting NDVI data to the LST-based reference grid")
}
