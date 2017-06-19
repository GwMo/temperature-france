# Parallelized extraction of data from a veloxed raster by polygons
parallel_extract <- function(veloxed_data, polys, fun, ncores) {
  # Assign polygons to groups of 1000
  groups <- ceiling(1:length(polys) / 1000)

  # Iterate in parallel over each group of polygons
  #   Project the polygons to match the dataset
  #   Extract and summarize the values for each polygon
  # Unlist the results and return the resulting vector
  mclapply(1:max(groups), function(i) {
    projected <- spTransform(polys[groups == i, ], veloxed_data$crs)
    veloxed_data$extract(projected, fun = fun)
  }, mc.cores = ncores) %>% do.call(rbind, .)
}
