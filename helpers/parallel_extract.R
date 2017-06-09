# Parallelized extraction of data from a veloxed raster by buffers
parallel_extract <- function(veloxed_data, buffers, fun, ncores) {
  # Assign each buffer to one of 600 groups
  groups <- ceiling(1:length(buffers) / (length(buffers) / 600))

  # Iterate in parallel over each group of buffers
  #   Project the buffers to match the dataset
  #   Extract and summarize the values for each buffer
  # Unlist the results and return the resulting vector
  mclapply(1:max(groups), function(i) {
    projected <- spTransform(buffers[groups == i, ], veloxed_data$crs)
    veloxed_data$extract(projected, fun = fun, na.rm = TRUE)
  }, mc.cores = ncores) %>% unlist
}
