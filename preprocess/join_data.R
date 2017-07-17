# Join all extracted data to the MODIS reference grid cells
#
# Load the MODIS reference grid
# For each year
# * For each month with data
#   - Load and properly scale the monthly NDVI data
#   - Extract the NDVI and QA data by the reference grid points
#   - Add the MODIS grid id and save

library(magrittr) # %>% pipe-like operator
library(dplyr)    # data manipulation e.g. joining tables
library(fst)      # fast serialization
library(sp)       # classes and methods for spatial data

# Set directories and load helper functions
file.path("~", "temperature-france", "helpers", "set_dirs.R") %>% source
file.path(helpers_dir, "report.R") %>% source


report("Joining all extracted data by location and date")


# Load the reference grid
report("Loading reference grid")
grid <- file.path(grids_dir, "modis_grid.rds") %>% readRDS

# Add latitude and longitude to the grid
epsg_4326 <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"
grid_4326 <- spTransform(grid, epsg_4326)
grid$latitude  <- grid_4326$y
grid$longitude <- grid_4326$x
rm(grid_4326, epsg_4326)

# Create a directory to hold all joined data
joined_dir <- file.path(extracts_dir, "joined")
dir.create(joined_dir, showWarnings = FALSE)

# Iterate over all model years
for (year in 2003:2003) {
  paste("Processing", year) %>% report

  # Create a directory to hold the joined data for the year
  joined_year_dir <- file.path(joined_dir, year)
  dir.create(joined_year_dir, showWarnings = FALSE)

  # Join data for each month in the year
  for (month in 1:12) {
    paste("  Processing", month.name[month]) %>% report

    # List all dates in the month
    from_date  <- paste(year, month,     "01", sep = "-") %>% as.Date
    next_month <- paste(year, month + 1, "01", sep = "-") %>% as.Date
    to_date <- next_month - 1
    dates <- seq.Date(from = from_date, to = to_date, by = 1) %>% as.character
    rm(from_date, next_month, to_date)

    # Create a template with all grid id - date pairs and all location data
    report("    Creating template to which data will be joined")
    template <- expand.grid(
      "modis_grid_id" = grid$id,
      "date" = dates,
      stringsAsFactors = FALSE
    ) %>% left_join(., as.data.frame(grid), by = c("modis_grid_id" = "id"))
    # 24 seconds
    # 1.17 GB
    # Entire year takes 192 seconds and is 13.7 GB

    ################
    # Join MODIS LST
    ################
    tmp_dir <- file.path(extracts_dir, "modis", year)
    for (sat in c("aqua", "terra")) {
      for(tod in c("day", "night")) {
        paste("    Joining MODIS", sat, tod, "LST") %>% report

        # Load the extracted LST data
        tmp <-
          paste0("modis_grid_", sat, "_lst_", tod, "_", year, "-%02d.fst") %>%
          sprintf(., month) %>%
          file.path(tmp_dir, .) %>%
          read.fst

        # Prefix the data columns with the satellite and time of day
        names(tmp) <-
          paste(sat, tod, "lst", sep = "_") %>%
          gsub("^lst", ., names(tmp))

        # Add the data to the template
        # If the grid ids and dates match then we can cbind the data columns to
        # the template; otherwise we have to join them
        if (
          all(tmp$modis_grid_id == template$modis_grid_id) &&
          all(tmp$date == template$date)
        ) {
          template <-
            select(tmp, -c(modis_grid_id, date)) %>%
            cbind(template, .)
        } else {
          template <- left_join(template, tmp, by = c("modis_grid_id", "date"))
        }
        rm(tmp)
      }
    }
    rm(tmp_dir, sat, tod)
    # 8 seconds per join -> 32 seconds
    # Adds 311 MB per join -> 2.41 GB

    #################
    # Join MODIS NDVI
    #################
    tmp_dir <- file.path(extracts_dir, "modis", year)
    for (sat in c("aqua", "terra")) {
      paste("    Joining MODIS", sat, "NDVI") %>% report

      # Load the extracted NDVI data
      tmp <-
        paste0("modis_grid_", sat, "_ndvi_", year, "-%02d.rds") %>%
        sprintf(., month) %>%
        file.path(tmp_dir, .) %>%
        readRDS

      # Drop the date column (NDVI data is monthly)
      tmp <- tmp[, names(tmp) != "date"]

      # Prefix the data columns with the satellite
      names(tmp) <- paste0(sat, "_ndvi") %>% gsub("^ndvi", ., names(tmp))

      # Join the data to the template
      template <- left_join(template, tmp, by = "modis_grid_id")
      rm(tmp)
    }
    rm(tmp_dir, sat)
    # 27 seconds per join -> 54 seconds
    # Adds 311 MB per join -> 3.04 GB

    ############################################
    # Join Meteo France SIM surface model output
    ############################################
    {
      report("    Joining Meteo France SIM surface model output")

      # Load the extracted SIM data
      tmp <-
        paste0("modis_grid_sim_", year, "-%02d.fst") %>%
        sprintf(., month) %>%
        file.path(extracts_dir, "sim", year, .) %>%
        read.fst

      # Prefix the data columns with "sim"
      names(tmp) <-
        gsub("^(?!modis_grid_id|date)", "sim_", names(tmp), perl = T)

      # Add the data to the template
      # If the grid ids and dates match then we can cbind the data columns to
      # the template; otherwise we have to join them
      if (
        all(tmp$modis_grid_id == template$modis_grid_id) &&
        all(tmp$date == template$date)
      ) {
        template <-
          select(tmp, -c(modis_grid_id, date)) %>%
          cbind(template, .)
      } else {
        template <- left_join(template, tmp, by = c("modis_grid_id", "date"))
      }
      rm(tmp)
    }
    # 7 seconds
    # Adds 1.4 GB -> 4.44 GB

    ################
    # Join Elevation
    ################
    {
      report("    Joining EU DEM v1.1 elevation")

      # Load the extracted elevation data
      tmp <- file.path(extracts_dir, "modis_1km_eu_dem_v11.rds") %>% readRDS

      # Prefix the data column with "eu_dem_v11"
      names(tmp) <- gsub("^elevation", "eu_dem_v11_elevation", names(tmp))

      # Add the data to the template
      template <- left_join(template, tmp, by = "modis_grid_id")
      rm(tmp)
    }
    # 32 seconds
    # Adds 156 MB -> 4.59 GB

    #################
    # Join Land Cover
    #################
    {
      report("    Joining Corine Land Cover")

      # Determine which CLC data year is closest to the current year
      clc_years <-
        list.files(extracts_dir, pattern = "^modis_1km_clc_") %>%
        gsub(".+_clc_(\\d{4}).+", "\\1", .) %>% as.numeric
      diffs <- abs(year - clc_years)
      clc_year <- which(diffs == min(diffs)) %>% first %>% clc_years[.]
      rm(clc_years, diffs)

      # Load the extracted CLC data for the closest CLC data year
      tmp <-
        paste0("modis_1km_clc_", clc_year, ".rds") %>%
        file.path(extracts_dir, .) %>%
        readRDS

      # Prefix the data columns with "clc" and the CLC data year
      names(tmp) <-
        paste0("clc_", clc_year, "_") %>%
        gsub("^(?!modis_grid_id)", ., names(tmp), perl = T)

      # Add the data to the template
      template <- left_join(template, tmp, by = "modis_grid_id")
      rm(tmp, clc_year)
    }
    # 38 seconds
    # Adds 2.34 GB -> 6.93 GB

    #################
    # Join Population
    #################
    {
      report("    Joining INSEE population")

      # Load the extracted population data
      tmp <- file.path(extracts_dir, "modis_1km_population.rds") %>% readRDS

      # Prefix the data column with "insee"
      names(tmp) <- gsub("^population", "insee_population", names(tmp))

      # Add the data to the template
      template <- left_join(template, tmp, by = "modis_grid_id")
      rm(tmp)
    }
    # 38 seconds
    # Adds 156 MB -> 7.08 GB

    # Save the joined data in rds format
    # fst with 50% compression is 4.5x faster for write (60 vs 272 seconds) and
    # 2.6x for read (25 vs 66 seconds) but uses 1.8x more disk (1.5 vs 0.817 GB)
    path <-
      as.Date(dates[1]) %>%
      format(., "modis_grid_%Y-%m.rds") %>%
      file.path(joined_year_dir, .)
    paste("    Saving to", path) %>% report
    saveRDS(template, path)
    # 272 seconds
    # Resulting file is 817 MB

    # Clear memory
    rm(template)
  }
}
rm(month, year, joined_dir, joined_year_dir)

report("Done")
