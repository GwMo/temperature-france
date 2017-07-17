# Define and ensure the presence of key directories

library(magrittr)

# Source data
data_dir <- file.path("~", "data") %>% path.expand
file.exists(data_dir) %>% stopifnot

# Model scripts, extracted data, and output
model_dir <- file.path("~", "temperature-france") %>% path.expand
file.exists(model_dir) %>% stopifnot

# Subdirs within the model dir
subdirs <- c(
  "helpers",  # helper scripts
  "grids",    # reference grid points
  "buffers",  # buffers around reference grid points
  "extracts", # extracted data
  "output"    # model output
)
for (subdir in subdirs) {
  path <- file.path(model_dir, subdir)   # path <- ~/temperature-france/helpers
  dir.create(path, showWarnings = FALSE) # ensure the path exists
  assign(paste0(subdir, "_dir"), path)   # helpers_dir <- path
}
rm(path, subdir, subdirs)

# Set the working directory to the model dir
setwd(model_dir)
