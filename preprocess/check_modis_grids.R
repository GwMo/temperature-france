# Check for misalignment between the different MODIS product grids
# If misalignment is small, use the Aqual LST grid as a standard reference grid

library("magrittr") # %>% pipe-like operator
library("raster")   # methods to manipulate gridded spatial data

# Set working directory
file.path("~", "data", "r") %>% path.expand %>% setwd

print("Checking alignement of MODIS grids:")

# Load the MODIS Aqua and Terra LST and NDVI grid rasters
products <- c(
  "aqua_lst",
  "aqua_ndvi",
  "terra_lst",
  "terra_ndvi"
)

grids <-
  sapply(products, function(p) {
    paste("modis", p, "grid.tif", sep = "_") %>% raster
  })

# Return the max difference between the origin, resolution, and coordinates of two grids
difference <- function(a, b) {
  d <- c(
    max(origin(a) - origin(b)),
    max(res(a) - res(b)),
    max(coordinates(a) - coordinates(b))
  )
  names(d) <- c("origin", "resolution", "coordinates")
  d
}

# Calculate misalignment between Aqua and Terra LST grids
print("Aqua LST vs Terra LST")
lst_vs_lst <- difference(grids$aqua_lst, grids$terra_lst)
print(lst_vs_lst)

# Calculate misalignment between Aqua and Terra NDVI grids
print("Aqua NDVI vs Tera NDVI")
ndvi_vs_ndvi <- difference(grids$aqua_ndvi, grids$terra_ndvi)
print(ndvi_vs_ndvi)

# Calculate misalignment between Aqua LST and NDVI grids
print("Aqua LST vs Aqua NDVI")
aqua_lst_vs_ndvi <- difference(grids$aqua_lst, grids$aqua_ndvi)
print(aqua_lst_vs_ndvi)

# Calculate misalignment between Terra LST and NDVI grids
print("Terra LST vs Terra NDVI")
terra_lst_vs_ndvi <- difference(grids$terra_lst, grids$terra_ndvi)
print(terra_lst_vs_ndvi)

# Combine the differences into a data frame
diffs <-
  rbind(lst_vs_lst, ndvi_vs_ndvi, aqua_lst_vs_ndvi, terra_lst_vs_ndvi) %>%
  as.data.frame

# If the grids are aligned to within 1 mm, use the aqua lst grid as the reference
if (max(diffs$coordinates) < 0.001) {
  print("Grids are aligned to within 1 mm")
  file.copy("modis_aqua_lst_grid.rds", "modis_grid.rds")
  print("Saved aqua lst grid as 'modis_grid.rds'")
} else {
  paste("Grids are misaligned by up to", max(diffs$coordinates), "m") %>% print
}
