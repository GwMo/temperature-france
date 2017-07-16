# Create small subsets of the reference grid and buffers for testing

library("magrittr")
library("sp")
library("raster")

# Set directories and load helper functions
file.path("~", "temperature-france", "helpers", "set_dirs.R") %>% source
file.path(helpers_dir, "report.R") %>% source


report("Creating test files")


# Create a subdir to hold the test files
test_dir <- file.path(model_dir, "test")
dir.create(test_dir, showWarnings = FALSE)

# france <- shapefile("~/data/ign/borders/france_epsg-2154.shp")

#######
# Grids
#######

report("Loading MODIS reference grid")

test_grids_dir <- file.path(test_dir, "grids")
dir.create(test_grids_dir, showWarnings = FALSE)
setwd(test_grids_dir)

pts <- file.path(grids_dir, "modis_grid.rds") %>% readRDS
mos <- file.path(grids_dir, "modis_grid_sinusoidal.tif") %>% brick
names(mos) <- c("x", "y", "row", "col", "mask")


# Full-size shapefiles
report("  Saving as shapefile")
shapefile(pts, "full_grid", overwrite = TRUE)
writeRaster(mos, "full_grid.tif", overwrite = TRUE)


# Calanques 15 x 15
report("  Creating small 15 x 15 grid")

small_rows <- 933:947
small_cols <- 876:890

small_mos <- crop(mos, extent(
  mos, min(small_rows), max(small_rows), min(small_cols), max(small_cols)
))
# plot(small_mos$mask)
# plot(spTransform(france, projection(small_mos)), add = TRUE)
writeRaster(small_mos, "small_mos.tif", overwrite = TRUE)

small_grid <- pts[(pts$row %in% small_rows & pts$col %in% small_cols), ]
# plot(spTransform(small_grid, projection(small_mos)), add = TRUE)
saveRDS(small_grid, "small_grid.rds")
shapefile(small_grid, "small_grid", overwrite = TRUE)

rm(small_mos, small_grid)


# Nord Pas de Calais 100 x 100
report("  Creating medium 100 x 100 grid")

medium_rows <- 1:100
medium_cols <- 525:624

medium_mos <- crop(mos, extent(
  mos, min(medium_rows), max(medium_rows), min(medium_cols), max(medium_cols)
))
# plot(medium_mos$mask)
# plot(spTransform(france, projection(medium_mos)), add = TRUE)
writeRaster(medium_mos, "medium_mos.tif", overwrite = TRUE)

medium_grid <- pts[(pts$row %in% medium_rows & pts$col %in% medium_cols), ]
# plot(spTransform(medium_grid, projection(medium_mos)), add = TRUE)
saveRDS(medium_grid, "medium_grid.rds")
shapefile(medium_grid, "medium_grid", overwrite = TRUE)

rm(medium_mos, medium_grid)


# 250 x 250 alps
report("  Creating large 250 x 250 grid")

large_rows <- 551:800
large_cols <- 771:1020

large_mos <- crop(mos, extent(
  mos, min(large_rows), max(large_rows), min(large_cols), max(large_cols)
))
# plot(large_mos$mask)
# plot(spTransform(france, projection(large_mos)), add = TRUE)
writeRaster(large_mos, "large_mos.tif", overwrite = TRUE)

large_grid <- pts[(pts$row %in% large_rows & pts$col %in% large_cols), ]
# plot(spTransform(large_grid, projection(large_mos)), add = TRUE)
saveRDS(large_grid, "large_grid.rds")
shapefile(large_grid, "large_grid", overwrite = TRUE)

rm(large_mos, large_grid)

rm(pts, mos)

#########
# Buffers
#########

report("Loading MODIS 1km square buffers")

test_buffers_dir <- file.path(test_dir, "buffers")
dir.create(test_buffers_dir, showWarnings = FALSE)
setwd(test_buffers_dir)

squares <- file.path(buffers_dir, "modis_square_1km.rds") %>% readRDS

# Full-size shapefiles
report("  Saving as shapefile")
shapefile(squares, "full_square_1km", overwrite = TRUE)


# Calanques 15 x 15
report("  Creating buffers for small 15 x 15 grid")

small <- squares[(squares$row %in% small_rows & squares$col %in% small_cols), ]
# plot(small)
# plot(france, add = TRUE)
saveRDS(small, "small_square_1km.rds")
shapefile(small, "small_square_1km", overwrite = TRUE)

rm(small)


# Nord Pas de Calais 100 x 100
report("  Creating buffers for medium 100 x 100 grid")

medium <- squares[squares$row %in% medium_rows & squares$col %in% medium_cols, ]
# plot(medium)
# plot(france, add = TRUE)
saveRDS(medium, "medium_square_1km.rds")
shapefile(medium, "medium_square_1km", overwrite = TRUE)

rm(medium)


# 250 x 250 alps
report("  Creating buffers for large 250 x 250 grid")

large <- squares[(squares$row %in% large_rows & squares$col %in% large_cols), ]
# plot(large)
# plot(france, add = TRUE)
saveRDS(large, "large_square_1km.rds")
shapefile(large, "large_square_1km", overwrite = TRUE)


report("Done")
