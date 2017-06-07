# Parallelized summarization of point data within polygons
parallel_over <- function(points, polygons, fun, ncores) {
  # Assign each polygon to one of 600 groups
  groups <- ceiling(1:length(polygons) / (length(polygons) / 600))

  # Iterate in parallel over each group of polygons
  #   Project the polygons to match the point data
  #   Extract and summarize the values for the points within each polygon
  # Unlist the results and return the resulting vector
  mclapply(1:max(groups), function(i) {
    projected <- spTransform(polygons[groups == i, ], proj4string(points))
    over(projected, points, fn = fun)
  }, mc.cores = ncores) %>% unlist
}
