library(raster)

source("helpers/report.R")

save_as_geotiff <- function(extracted_data, grid_pixels, tif_path) {
  # Confirm the extracted data is in the same order as the grid pixels
  if (identical(grid_pixels$index, extracted_data$index)) {
    char_cols <- function(dt) {
      cols <- colnames(dt)[sapply(dt, class) == "character"]

      # Warn if any character columns are not an id
      if (any(!grepl("(^|_)id$", cols))) {
        report(paste("Raster cannot contain strings; excluding", cols))
      }

      cols
    }

    non_char_cols <- function(dt) {
      setdiff(colnames(dt), char_cols(dt))
    }

    # Add all non-character columns except the grid index to the grid pixels
    # Rasters cannot contain strings so character columns would be coerced to NA
    cols_to_add <- setdiff(non_char_cols(extracted_data), "index")
    grid_pixels@data[cols_to_add] <- extracted_data[, cols_to_add, with = FALSE]

    # Convert to a raster and save
    writeRaster(stack(grid_pixels), tif_path, overwrite = TRUE)
  } else {
    stop("extracted_data$index != grid_pixels$index")
  }
}
