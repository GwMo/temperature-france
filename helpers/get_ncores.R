# Get the number of cores available
get_ncores <- function() {
  # Check whether the script is running in an OAR job
  if (!is.na(Sys.getenv("OAR_NODEFILE", unset = NA))) {
    # If yes, infer the number of cores from an OAR env variable
    ncores <- Sys.getenv("OAR_NODEFILE") %>% readLines %>% length
  } else {
    # Otherwise use the OS-reported number of cores
    ncores <- detectCores()
  }
}
