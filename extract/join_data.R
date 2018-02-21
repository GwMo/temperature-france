# Join all extracted data to the LST 1 km reference grid ----------------------
#
# Load the LST 1 km reference grid
# For each year
# * For each month
#   - Create a table (mod3) listing all grid id - date pairs
#   - Join each extracted dataset to the table
#   - Save the table

library(magrittr)   # %>% pipe-like operator
library(parallel)   # parallel computation
library(data.table) # fast data joining
library(fst)        # fast serialization
library(sp)         # classes and methods for spatial data
library(gstat)      # IDW interpolation
library(FNN)        # k-nearest neighbours, needed for nearest-by-day join
import::from(aodlur, makepointsmatrix, nearestbyday) # nearest-by-day join

# Load helper functions
source("helpers/constants.R")
source("helpers/report.R")
source("helpers/get_ncores.R")

report("")
report("Joining all data to create mod3, mod2, and mod1")
report("---")

# Setup -----------------------------------------------------------------------

# Number of cores to use for parallel computation
ncores <- get_ncores()

# Define functions ------------------------------------------------------------

# Add columns to a table by reference (does not copy the table)
add_by_reference <- function(main_table, new_data) {
  if (safe_to_add(main_table, new_data)) {
    # Drop the key columns from the new data, then add the remaining columns to the main table
    new_data[, key(main_table) := NULL]
    main_table[, names(new_data) := new_data]
    invisible(main_table)
  } else {
    stop("Not safe to add by reference")
  }
}

# List the columns two data.tables have in common
common_cols <- function(x, y) {
  intersect(names(x), names(y))
}

# Return a table listing all grid id - date combinations in a month
cell_days_table <- function(grid_table, month_start) {
  # List all date - grid id combinations along with the grid data
  cell_days <-
    CJ("date" = dates_in_month(month_start), "id" = grid_table$id) %>%
    grid_table[., on = "id"]

  # Prefix the grid table data columns, set keys, and reorder columns
  cell_days %>% setnames(old = names(grid_table), new = paste0("lst_1km_", names(grid_table)))
  cell_days %>% setkeyv(cols = c("date", "lst_1km_id"))
  cell_days %>% setcolorder(c(key(cell_days), not_key(cell_days)))

  # Return the table
  cell_days
}

# Find the data year closest to the specified year
closest_data_year <- function(data_years, year) {
  diffs <- abs(year - data_years)
  data_years[diffs == min(diffs)][1] # return the first match e.g. (2000, 2002) vs 2001 -> 2000
}

# List all dates in the month of the passed date
dates_in_month <- function(d) {
  month_start <- format(d, "%Y-%m-01") %>% as.Date
  seq.Date(
    from = month_start,
    to = seq.Date(month_start, by = "month", length.out = 2)[2] - 1,
    by = 1
  ) %>% format("%Y-%m-%d")
}

# Join data to a table by reference (does not copy the table)
join_by_reference <- function(main_table, new_data) {
  if (safe_to_join(main_table, new_data)) {
    # Find the table rows that match the new data and update them by reference
    cols_to_add <- not_key(new_data)
    main_table[new_data, (cols_to_add) := mget(paste0("i.", cols_to_add)), on = key(new_data)]
    invisible(main_table)
  } else {
    stop("Not safe to join by reference")
  }
}

# List the columns of a data.table that are not keys
not_key <- function(data_table) {
  setdiff(names(data_table), key(data_table))
}

# Add a prefix to the name of every non-key column in a data.table
prefix_non_key_cols <- function(data_table, prefix) {
  cols <- not_key(data_table)
  setnames(data_table, old = cols, new = paste(prefix, cols, sep = "_"))
  invisible(data_table)
}

# Check whether it's safe to add new data to a table by reference
safe_to_add <- function(main_table, new_data) {
  all(
    # Confirm the data is safe to join (it won't overwrite existing columns)
    safe_to_join(main_table, new_data),

    # Confirm the key columns are identical i.e. the data is in the same grid id - date order
    identical(
      main_table[, key(main_table), with = FALSE],
      new_data[, key(new_data), with = FALSE]
    )
  )
}

# Check whether it's safe to join new data to table by reference
safe_to_join <- function(main_table, new_data) {
  # Confirm the data will not overwrite existing columns in the main table
  identical(key(new_data), common_cols(main_table, new_data))
}

# Load MODIS 1 km grid --------------------------------------------------------

report("Loading reference grid data")
grid_table <-
  file.path(constants$grid_lst_1km_dir, "grid_lst_1km.rds") %>%
  readRDS %>%
  as.data.table %>%
  setcolorder(c("id", "row", "col", "index", "latitude", "longitude", "x", "y"))

# Join data for each year -----------------------------------------------------
# for (year in constants$model_years) {
for (year in c(2008, 2000:2007, 2009:2016)) {
  # Create a directory to hold the joined data
  year_dir <- file.path(constants$grid_lst_1km_dir, "joined", year)
  dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
  paste(year, year_dir, sep = ": ") %>% report

  # Create mod3: all data -----------------------------------------------------
  # Process data by month (entire year uses too much memory)
  for (month in 1:12) {
    month_start <- paste0(year, "-%02d-01") %>% sprintf(month) %>% as.Date
    paste0("  ", format(month_start, "%B")) %>% report

    # mod3 = all data for all cell-days ---------------------------------------
    report("    Creating mod3")
    {
      # List all cell-days (5 sec, 1.2 GB)
      # (Entire year would take 30 seconds and be 11.9 GB)
      report("      Listing cell-days in month")
      mod3 <- cell_days_table(grid_table, month_start)

      # Join Elevation (5 seconds, 0.1 GB)
      report("      Joining EU DEM v1.1 elevation")
      for (prefix in "eu_dem_v11") {
        # Load the extracted elevation data
        tmp <-
          file.path(constants$grid_lst_1km_extracts_dir, "grid_lst_1km_eu_dem_v11.rds") %>%
          readRDS

        # Prefix the data column with "eu_dem_v11"
        prefix_non_key_cols(tmp, prefix)

        # Join the data to mod3
        join_by_reference(mod3, tmp)

        rm(tmp, prefix)
      }

      # Join Population (5 seconds, 0.1 GB)
      report("      Joining INSEE population")
      for (prefix in "insee") {
        # Load the extracted population data
        tmp <-
          file.path(constants$grid_lst_1km_extracts_dir, "grid_lst_1km_insee_pop.rds") %>%
          readRDS

        # Prefix the data column with "insee"
        prefix_non_key_cols(tmp, prefix)

        # Join the data to mod3
        join_by_reference(mod3, tmp)

        rm(tmp, prefix)
      }

      # Join Land Cover (10 seconds, 1.4 GB)
      report("      Joining Corine Land Cover")
      for (prefix in "clc") {
        # Load the extracted CLC data for the data year closest to the current year
        tmp <-
          closest_data_year(constants$clc_years, year) %>%
          paste0("grid_lst_1km_clc_", ., ".rds") %>%
          file.path(constants$grid_lst_1km_extracts_dir, .) %>%
          readRDS

        # Prefix the data columns with "clc"
        prefix_non_key_cols(tmp, prefix)

        # Join the data to mod3
        join_by_reference(mod3, tmp)

        rm(tmp, prefix)
      }

      # Join climate type (5 seconds, 0.1 GB)
      report("      Joining Joly (2010) climate type")
      for (prefix in "joly_2010") {
        # Load the extracted data
        tmp <-
          file.path(constants$grid_lst_1km_extracts_dir, "grid_lst_1km_joly_climate_type.rds") %>%
          readRDS

        # Prefix the data column with "insee"
        prefix_non_key_cols(tmp, prefix)

        # Join the data to mod3
        join_by_reference(mod3, tmp)

        rm(tmp, prefix)
      }

      # Join Meteo France SIM model output (15 seconds, 1.4 GB)
      report("      Joining Meteo France SIM surface model output")
      for (prefix in "sim") {
        # Load the extracted SIM data
        tmp <-
          paste0("grid_lst_1km_sim_", format(month_start, "%Y-%m"), ".fst") %>%
          file.path(constants$grid_lst_1km_extracts_dir, year, .) %>%
          read_fst(as.data.table = TRUE)

        # Drop variables we don't need
        sim_vars <- c("temperature_min", "temperature_mean", "temperature_max")
        tmp <- tmp[, c(key(tmp), sim_vars), with = FALSE]
        rm(sim_vars)

        # Prefix the data columns with "sim"
        prefix_non_key_cols(tmp, prefix)

        # Add the data to mod3 by reference
        add_by_reference(mod3, tmp)

        rm(tmp, prefix)
      }

      # Join MODIS NDVI (25 seconds, 0.4 GB)
      for (sat in constants$modis_satellites) {
        # Find the extracted data for the month
        path <-
          paste0("grid_lst_1km_", sat, "_ndvi_", format(month_start, "%Y-%m"), ".rds") %>%
          file.path(constants$grid_lst_1km_extracts_dir, year, .)
        if (file.exists(path)) {
          paste("      Joining MODIS", sat, "NDVI") %>% report
          tmp <- readRDS(path)
        } else {
          paste("      No MODIS", sat, "NDVI data; filling with NA") %>% report

          # Get the names of the columns that would have been added had data existed
          ndvi_cols <-
            paste0("grid_lst_1km_", sat, "_ndvi") %>%
            list.files(
              constants$grid_lst_1km_extracts_dir, full.names = TRUE, pattern = ., recursive = TRUE
            ) %>%
            magrittr::extract(1) %>%
            readRDS %>%
            names

          # Make a dummy data.table with grid ids but no data
          # Don't include dates b/c real data doesn't have dates - NDVI data is monthly
          tmp <- mod3[date == mod3$date[1], "lst_1km_id", with = FALSE]
          tmp %>% setkey("lst_1km_id")
          tmp[, setdiff(ndvi_cols, names(tmp)) := NA]
          rm(ndvi_cols)
        }
        rm(path)

        # Drop the date column (NDVI data is monthly)
        tmp[, month := NULL]

        # Prefix the data columns with the satellite
        prefix_non_key_cols(tmp, sat)

        # Join the data to mod3 by reference
        join_by_reference(mod3, tmp)
        rm(tmp, sat)
      }

      # Join MODIS LST (90 seconds, 2.2 GB)
      for (sat in constants$modis_satellites) {
        # Find the extracted data for the month
        data_path <-
          paste0("grid_lst_1km_", sat, "_lst_", format(month_start, "%Y-%m"), ".rds") %>%
          file.path(constants$grid_lst_1km_extracts_dir, year, .)
        if (file.exists(data_path)) {
          paste("      Joining MODIS", sat, "LST") %>% report

          # Load the extracted LST data
          tmp <- readRDS(data_path)
        } else {
          paste("      No MODIS", sat, "LST data; filling with NA") %>% report

          # Get the names of the columns that would have been added had data existed
          # lst_cols <-
          #   paste0("grid_lst_1km_", sat, "_lst") %>%
          #   list.files(
          #     constants$grid_lst_1km_extracts_dir, full.names = TRUE, pattern = ., recursive = TRUE
          #   ) %>%
          #   first %>%
          #   readRDS %>%
          #   names
          lst_cols <- c("date", "lst_1km_id", "day_lst", "day_lst_error", "night_lst",
                        "night_lst_error", "emissivity", "day_emis_error", "night_emis_error")

          # Make a table listing every cell-day with NA-filled columns for LST data
          tmp <- mod3[, key(mod3), with = FALSE]
          tmp[, setdiff(lst_cols, names(tmp)) := NA]
          rm(lst_cols)
        }
        rm(data_path)

        # Prefix the non-key data columns with the satellite name
        prefix_non_key_cols(tmp, sat)

        # Add the data to mod3 by reference
        add_by_reference(mod3, tmp)
        rm(tmp, sat)
      }

      # Remove LST for cells that are > 33% water or have NDVI < 0 (5 seconds)
      report("      Removing LST observations for cells with > 33% water or NDVI < 0")
      for (sat in constants$modis_satellites) {
        for (overpass in constants$modis_overpasses) {
          lst_col <- paste(sat, overpass, "lst", sep = "_")
          ndvi_col <- paste(sat, "ndvi", sep = "_")
          mod3[clc_water > 0.33, (lst_col) := NA]
          mod3[get(ndvi_col) < 0, (lst_col) := NA]
        }
        rm(lst_col, ndvi_col, sat, overpass)
      }

      # Interpolate Meteo France weather data by IDW (75 seconds, 0.5 GB)
      report("      Interpolating Meteo France weather station data by IDW")
      for (prefix in "meteo") {
        # Load the cleaned station data
        meteo <-
          file.path(constants$work_dir, "meteo_france", "observations", "clean") %>%
          list.files(full.names = TRUE, pattern = paste0("meteo_data_", year, "-clean\\.rds")) %>%
          readRDS

        # Drop observations not in the current month
        meteo <- meteo[date %in% unique(mod3$date), ]

        # Drop variables we don't need
        key_cols <- c("date", "insee_id", "latitude", "longitude")
        info_cols <- c("station_type", "station_elevation")
        var_cols <- c("temperature_min", "temperature_mean", "temperature_max")
        meteo <- meteo[, c(key_cols, info_cols, var_cols), with = FALSE]

        # Convert the observation locations to EPSG:2154 (Lambert-93) and reorder the columns
        meteo[, c("x", "y") := (
          meteo[, c("longitude", "latitude")] %>%
            setnames(c("x", "y")) %>%
            SpatialPoints(proj4string = CRS(constants$epsg_4326)) %>%
            spTransform(constants$epsg_2154) %>%
            as.data.table
        )]
        meteo %>% setcolorder(c(key_cols, "x", "y", info_cols, var_cols))

        # For each cell-day, predict each variable using inverse distance weighted interpolation
        # 75 seconds, +0.4 GB -> 8.1 GB
        for (v in var_cols) {
          paste0("        ", v) %>% report

          # Filter out observations where the variable is missing and split by date
          splits <-
            meteo[!is.na(get(v)), c("date", "x", "y", v), with = FALSE] %>%
            split(by = "date")

          # Run the IDW interpolation for all dates in parallel
          # 25 seconds
          idw <- mclapply(splits, function(splt, variable, grid_pts) {
            # Predict for each grid point and round
            predictions <-
              paste0(variable, " ~ 1") %>%
              as.formula %>%
              gstat(formula = ., locations = ~ x + y, data = splt) %>%
              predict(newdata = grid_pts) %$%
              var1.pred %>%
              round(1)

            # Add the date and lst 1 km id and return
            result <- data.table(date = splt$date[1], lst_1km_id = grid_pts$id)
            result[, paste0("idw_", v) := predictions]
          }, mc.cores = ncores, variable = v, grid_pts = grid_table) %>% rbindlist

          # Sort predictions by date and lst 1 km id and add to mod3 by reference
          idw %>% setkeyv(cols = c("date", "lst_1km_id"))
          add_by_reference(mod3, idw)

          # Cleanup
          rm(idw, splits, v)
        }
        rm(key_cols, info_cols, var_cols, prefix) # don't remove meteo; will use for mod1
      }

      # Save mod3
      #  90 seconds ( 35 to read), 1063 MB for fst
      # 280 seconds (100 to read),  915 MB for RDS
      mod3_path <- format(month_start, "grid_lst_1km_mod3_%Y-%m.fst") %>% file.path(year_dir, .)
      paste("      Saving", basename(mod3_path)) %>% report
      threads_fst(ncores)
      write_fst(mod3, mod3_path, compress = 100)
      rm(mod3_path)
    }

    # mod2 and mod1 -----------------------------------------------------------
    for (sat in constants$modis_satellites) {
      for (overpass in constants$modis_overpasses) {
        lst_col <- paste(sat, overpass, "lst", sep = "_")
        paste("    Creating mod2 and mod1 for", lst_col) %>% report

        # mod2 = cell-days with LST data
        {
          # Subset cell-days that have LST for the sattelite overpass
          mod2 <- mod3[!is.na(get(lst_col)), ]

          if (nrow(mod2) == 0) {
            report("      No LST data, skipping")
            next
          }

          # Drop columns from the other satellite or a different overpass of this satellite
          other_sat <- setdiff(constants$modis_satellites, sat)
          other_pass <- setdiff(constants$modis_overpasses, overpass) %>% paste(sat, ., sep = "_")
          pattern <- paste0("^(", other_sat, "|", other_pass, ")_") # "^(terra|aqua_night)_"
          drop_cols <- colnames(mod2)[colnames(mod2) %like% pattern]
          mod2[, (drop_cols) := NULL]
          rm(other_sat, other_pass, pattern, drop_cols)

          # Save mod2
          #  35 seconds (15 to read), 450 MB for fst
          # 120 seconds (40 to read), 435 MB for terra
          mod2_path <-
            paste0("grid_lst_1km_mod2_", sat, "_", overpass, "_%Y-%m.fst") %>%
            format(month_start, .) %>%
            file.path(year_dir, .)
          paste("      Saving", basename(mod2_path)) %>% report
          threads_fst(ncores)
          write_fst(mod2, mod2_path, compress = 100)
          rm(mod2_path)
        }

        # mod1 = cell-days with LST and temperature observation
        {
          # Match grid data to meteo station observations
          report("      Joining meteo station observations to LST by date and location")
          {
            # Get the data needed to join by date
            # aodlur::nearestbydate renames and changes column types of the data it operates on
            lst_points <- mod2[!is.na(get(lst_col)), .(date, lst_1km_id, lst_1km_x, lst_1km_y)]
            obs_points <- meteo[, .(date, insee_id, x, y)]

            # Set column names
            # aodlur::nearestbydate requires date column to be named "day"
            setnames(lst_points, "date", "day")
            setnames(obs_points, "date", "day")

            # Match each observation to the closest LST measure (within 1.5 km) for that date
            matches <- nearestbyday(
              jointo.pts = makepointsmatrix(obs_points, "x", "y", "insee_id"),
              joinfrom.pts = makepointsmatrix(lst_points, "lst_1km_x", "lst_1km_y", "lst_1km_id"),
              jointo = obs_points,
              joinfrom = lst_points,
              jointovarname = "insee_id",
              joinfromvarname = "lst_1km_id",
              joinprefix = "lst_1km_id",
              valuefield = "lst_1km_id",
              knearest = 9,
              maxdistance = 1500
            )
            rm(lst_points, obs_points)

            # Drop unmatched observations and all but the id and date columns and reset column types
            # aodlur::nearestbday coerces id columns to character
            matches <- matches[!is.na(lst_1km_id), .(date = day, insee_id, lst_1km_id)]
            matches[, insee_id := as.integer(insee_id)]

            # Select the meteo observations and grid data that were matched
            matched_meteo <- meteo[matches, on = c("date", "insee_id")]
            matched_cell_days <- mod2[matches, on = c("date", "lst_1km_id")]
            rm(matches)

            # Add the grid data to the meteo observations and set keys
            mod1 <- matched_meteo[matched_cell_days, on = c("date", "insee_id", "lst_1km_id")]
            mod1 %>% setkeyv(c("date", "insee_id"))
            rm(matched_cell_days, matched_meteo)
          }

          # Save mod1
          mod1_path <-
            paste0("grid_lst_1km_mod1_", sat, "_", overpass, "_%Y-%m.rds") %>%
            format(month_start, .) %>%
            file.path(year_dir, .)
          paste("      Saving", basename(mod1_path)) %>% report
          threads_fst(ncores)
          saveRDS(mod1, mod1_path)
          rm(mod1, mod1_path)
        }
      }
    }
    rm(meteo, mod2, mod3)
  }
}

report("Done")
