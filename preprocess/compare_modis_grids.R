# Check for misalignment between the different MODIS product grids

library("magrittr")
library("raster")

file.path("~", "data", "r") %>% path.expand %>% setwd

aqua_lst = raster("modis_aqua_lst_grid.tif")
aqua_ndvi = raster("modis_aqua_ndvi_grid.tif")
terra_lst = raster("modis_terra_lst_grid.tif")
terra_ndvi = raster("modis_terra_ndvi_grid.tif")

print_diff <- function(a, b) {
  if (all.equal(a, b, tolerance = 0, res = TRUE, orig = TRUE, showwarning = FALSE)) {
    print("  Identical")
  } else {
    (origin(a) - origin(b)) %>%
      max %>%
      paste("  Max origin difference : ", .) %>%
      print
    (res(a) - res(b)) %>%
      max %>%
      paste("  Max resolution difference : ", .) %>%
      print
    (coordinates(a) - coordinates(b)) %>%
      max %>%
      paste("  Max xy difference : ", .) %>%
      print
  }
}

print("Aqua LST vs Terra LST")
print_diff(aqua_lst, terra_lst)

print("")

print("Aqua NDVI vs Tera NDVI")
print_diff(aqua_ndvi, terra_ndvi)

print("")

print("Aqua LST vs Aqua NDVI")
print_diff(aqua_lst, aqua_ndvi)

print("")

print("Terra LST vs Terra NDVI")
print_diff(terra_lst, terra_ndvi)
