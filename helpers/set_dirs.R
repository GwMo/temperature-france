# Set project directories

# Base directory
if (.Platform$OS.type == "windows") {
  base_dir <- file.path("P:/")
} else {
  base_dir <- path.expand(file.path("~/P055.Ian"))
}
stopifnot(file.exists(base_dir))

# Source data
data_dir <- file.path(base_dir, "data")
stopifnot(file.exists(data_dir))

# Model code
model_dir <- file.path(base_dir, "temperature-france")
stopifnot(file.exists(model_dir))

# Model output
output_dir <- file.path(data_dir, "temperature-france-output")
dir.create(output_dir, showWarnings = FALSE)

# Working directory
setwd(model_dir)
