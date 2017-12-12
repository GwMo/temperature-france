# Determine the number of cores to use for parallel computations based on the system
get_ncores <- function() {
  # In an OAR job, get the number of allocated cores from an env variable
  if (!is.na(Sys.getenv("OAR_NODEFILE", unset = NA))) {
    Sys.getenv("OAR_NODEFILE") %>% readLines %>% length
  }
  # On zbeast, use 36 cores (56 total)
  else if (Sys.info()["nodename"] == "zbeast") {
    36
  }
  # On itai-ws2, use 24 cores (32 total)
  else if (Sys.info()["nodename"] == "zbeast") {
    24
  }
  # On Windows, parallel execution isn't supported
  else if (Sys.info()["sysname"] == "Windows") {
    1
  }
  # On all other systems, use one or all but one cores
  else {
    max(1, detectCores() - 1)
  }
}
