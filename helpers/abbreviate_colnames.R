# Shorten the names of a data table
abbreviate_colnames <- function(data_table) {
  # Get the column names
  cols <- colnames(data_table)

  # Abbreviate variables
  cols <- sub("elevation", "elev", cols)
  cols <- sub("emissivity", "emis", cols)
  cols <- sub("error", "err", cols)
  cols <- sub("insee_id", "station_id", cols)
  cols <- sub("latitude", "lat", cols)
  cols <- sub("longitude", "lon", cols)
  cols <- sub("population", "pop", cols)
  cols <- sub("usefulness", "quality", cols)
  cols <- sub("temperature_", "t", cols)

  # Abbreviate prefixes
  cols <- sub("eu_dem_v11_", "", cols)
  cols <- sub("insee_", "", cols)
  cols <- sub("joly_2010_", "", cols)
  cols <- sub("station_", "stn_", cols)

  # Return the abbreviated names
  cols
}
