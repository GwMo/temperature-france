# Constants for use in all scripts
constants <- list(
  "clc_years" = c(2000, 2006, 2012),
  "model_years" = 2000:2016,
  "modis_satellites" = c("aqua", "terra"),
  "modis_overpasses" = c("day", "night")
)

# Directories
if (.Platform$OS.type == "windows") {           # BGU desktop
  base_dir <- "P:/"
} else if (Sys.info()["sysname"] == "Darwin") { # Mac laptop
  base_dir <- "~/code"
} else {                                        # BGU Rstudio server
  base_dir <- "/media/qnap/Projects/P055.Ian"
}
constants$data_dir <- file.path(base_dir, "0.raw")
constants$work_dir <- file.path(base_dir, "1.work")
constants$code_dir <- file.path(base_dir, "2.scripts")
stopifnot(file.exists(constants$data_dir))
stopifnot(file.exists(constants$work_dir))
stopifnot(file.exists(constants$code_dir))
rm(base_dir)

# Directories for the LST 1 km grid and associated data
constants$grid_lst_1km_dir <- file.path(constants$work_dir, "grid_lst_1km")
dir.create(constants$grid_lst_1km_dir, showWarnings = FALSE)
for (d in c("extracts", "joined", "results")) {
  constant_name <- paste0("grid_lst_1km_", d, "_dir")
  constants[[constant_name]] <- file.path(constants$grid_lst_1km_dir, d)
  dir.create(constants[[constant_name]], showWarnings = FALSE)
  rm(constant_name, d)
}

# EPSG:2154 = RGF93 / Lambert-93 (official projection for continental France)
constants$epsg_2154 <- "+proj=lcc +lat_1=44 +lat_2=49 +lat_0=46.5 +lon_0=3 +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m +no_defs"

# EPSG:3035 = ETRS89 / LAEA Europe (equal-area projection for mapping at the scale of Europe)
constants$epsg_3035 <- "+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs"

# EPSG:4326 = WGS84 (GPS latitude / longitude)
constants$epsg_4326 <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"

# MODIS sinusoidal (the projection of MODIS Aqua and Terra products)
# Copied from a MODIS Aqua LST data file
constants$proj_sinu <- "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"

# Lambert II étendu (deprecated projection for continental France; superseded by EPSG:2154)
# USE CAUTION when data claim to use this projection. May also be referred to as Lambert II Carto.
# Easily confused with Lambert Zone II, which differs only in using a false northing of 200,000
# rather than 2,200,000. Confusion may also arise from the fact that Lambert II étendu uses Paris
# (rather than Greenwich) as the prime meridian and gradians (rather than degrees). Equivalent to
# EPSG:27572 "NTF (Paris) / Lambert Zone II" and ESRI:102582 "NTF France II (degrees)" - note that
# the latter uses Greenwich as the prime meridian and Paris as the central meridian. Also note that
# proj4 does not support gradians, so the proj4 string for Lambert II étendu must use degrees.
# Transformatino accuracy from Lambert II étendu to other projections can be improved by using the
# IGN's NTF to RGF93 shift grid file. To use this file, check whether ntf_r93.gsb exists in your
# proj4 library(likely /usr/share/proj/ or /usr/local/share/proj). If it does not, download and add
# it (see https://grasswiki.osgeo.org/wiki/IGNF_register_and_shift_grid_NTF-RGF93)
constants$lambert_etendu <- "+proj=lcc +nadgrids=ntf_r93.gsb,null +lat_1=46.8 +lat_0=46.8 +lon_0=0 +k_0=0.99987742 +x_0=600000 +y_0=2200000 +a=6378249.2 +b=6356515 +towgs84=-168,-60,320,0,0,0,0 +pm=paris +units=m +no_defs"

# Log file (if it was passed as an argument to Rscript)
args <- commandArgs(TRUE)
pattern <- "^--log=(.+\\.log)$"
if (any(grepl(pattern, args))) {
  constants$logfile <- sub(pattern, "\\1", args[grepl(pattern, args)])
}
rm(args, pattern)

# Set working directory
setwd(constants$code_dir)
