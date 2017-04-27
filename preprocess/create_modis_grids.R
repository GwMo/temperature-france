# Create a reference grid covering metropolitan France (except Corsica) with an
# origin and resolution matching the MODIS LST data

library("magrittr")  # %>% pipe-like operator
library("sp")        # classes and methods for spatial data
library("rgdal")     # wrapper for GDAL and proj.4 to manipulate spatial data
                     # (GDAL and proj.4 must be present)
library("raster")    # methods to manipulate gridded spatial data
library("gdalUtils") # extends rgdal and raster to manipulate HDF4 files
                     # (GDAL must have been built with HDF4 support)

# Set the data dir and working dir
data_dir <- file.path("~", "data") %>% path.expand
setwd(file.path(data_dir, 'r'))

# Load a shapefile of metropolitan France in the EPSG:2154 projection
france <- file.path(data_dir, "ign", "france_epsg-2154.shp") %>% shapefile

# Set paths to MODIS Aqua and Terra LST and NDVI products
products <- list(
  "modis_aqua_lst" = file.path(data_dir, "modis", "aqua", "MYD11A1.006"),
  "modis_aqua_ndvi" = file.path(data_dir, "modis", "aqua", "MYD13A3.006"),
  "modis_terra_lst" = file.path(data_dir, "modis", "terra", "MOD11A1.006"),
  "modis_terra_ndvi" = file.path(data_dir, "modis", "terra", "MOD13A3.006")
)

# For each product, create a reference grid with a point at the center of every
# pixel that falls within metropolitan France
for (product in names(products)) {
  paste("Creating ", product, " grid") %>% print

  # Find the tiles for the first date of the product
  print("  Finding tiles")
  tiles <-
    products[[product]] %>%
    list.dirs(., recursive = FALSE) %>%
    .[1] %>%
    list.files(., full.names = TRUE, pattern = "\\.hdf$")

  # Load a LST or NDVI dataset for each tile
  print("  Loading tiles as rasters")
  rasters <-
    lapply(tiles, function(tile) {
      if (grepl("_lst$", product)) {
        sds <- "LST_Night_1km$"
      } else {
        sds <- "1 km monthly NDVI$"
      }
      get_subdatasets(tile) %>% .[grepl(sds, .)] %>% raster
    })

  # Create a RasterBrick the size of France aligned to the tiles in EPSG:2154
  print("  Merging rasters")
  grid <-
    do.call(merge, rasters) %>%       # mosaic the rasters together
    projectExtent(., france) %>%      # project to EPSG:2154 (we don't need the data so just project the extent)
    crop(., france, snap = "out") %>% # crop to France, snapping outwards to include cells that partially overlap
    brick(., nl = 3)                  # create a RasterBrick with 3 layers

  # Add layer names and cell coordinates
  names(grid) <- c("x", "y", "mask")
  values(grid$x) <- xFromCell(grid, 1:ncell(grid))
  values(grid$y) <- yFromCell(grid, 1:ncell(grid))

  # Add a mask of France: cells with their center in France have a value of 1
  print("  Masking France")
  grid$mask <- setValues(grid$mask, 1) %>% mask(., france)

  # Save the grid to GeoTIFF for reference
  filename <- paste(product, "grid.tif", sep = "_")
  paste("  Saving to ", filename) %>% print
  writeRaster(grid, filename)

  # Convert the grid to a spatial points dataframe
  print("  Converting to sptial points dataframe")
  grid <- as.data.frame(grid)
  coordinates(grid) <- ~ x + y
  projection(grid) <- projection(france)

  # Delete all points that fall outside of France
  print("  Removing points outside France")
  grid <- grid[!is.na(grid$mask), ]

  # Save the spatial points dataframe to an rds file
  filename <- paste(product, "grid.rds", sep = "_")
  paste("  Saving to ", filename) %>% print
  saveRDS(grid, filename)
}
