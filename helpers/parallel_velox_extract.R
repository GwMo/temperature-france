library(parallel)
library(velox)

source("helpers/split_into_n.R")

# Parallelized extraction of data from a veloxed raster by polygons
parallel_velox_extract <- function(vlx, splits, fun, ncores) {
  # Ensure splits is a list of polygons
  if (!is.list(splits)) {
    splits <- split_into_n(splits, ncores)
  }

  # Ensure the polygons are in the same projection as the data
  splits <- mclapply(splits, function(polys, proj4) {
    if (!identical(proj4string(polys), proj4)) {
      spTransform(polys, proj4)
    } else {
      polys
    }
  }, mc.cores = ncores, proj4 = vlx$crs)

  # For each split, crop the velox to the extent of the polygons and add to the split
  # This should take no more than 45 seconds and is *much* more memory-efficient b/c it avoids
  # having to make many copies of the entire velox object when extracting in parallel
  splits <- lapply(splits, function(polys, vx) {
    # Make a copy of the veloxed data and crop it to the polygons' extent
    # Must copy before cropping b/c the extents of different polygon groups may overlap
    vx <- vx$copy()
    vx$crop(polys)

    # Return both the polygons and the cropped velox
    list("polys" = polys, "vlx" = vx)
  }, vx = vlx)

  # In parallel, extract and summarize the values for each polygon
  result <- mclapply(splits, function(splt, fun) {
    splt$vlx$extract(splt$polys, fun = fun)
  }, mc.cores = ncores, fun = fun)

  # Return the result as a vector
  do.call(rbind, result)[, 1]
}
