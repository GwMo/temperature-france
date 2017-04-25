# Check for misalignment between the different MODIS product grids

library("magrittr")
library("raster")

file.path("~", "data", "r") %>% path.expand %>% setwd

# Load the MODIS Aqua and Terra LST and NDVI grid rasters
products <-
  c(
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
  d <-
    c(
      max(origin(a) - origin(b)),
      max(res(a) - res(b)),
      max(coordinates(a) - coordinates(b))
    )
  names(d) <- c("origin", "resolution", "coordinates")
  d
}

# Calculate misalignment between Aqua and Terra LST grids
print("Difference in alignment between MODIS grids:")

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
  print("Saving aqua lst grid as 'modis_grid.rds'")
  file.copy("modis_aqua_lst_grid.rds", "modis_grid.rds")
} else {
  print(paste("Grids are misaligned by up to", max(diffs$coordinates), "m"))
}
