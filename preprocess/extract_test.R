library("magrittr")
library("sp")
library("raster")
library("velox")
library("pryr")

data_dir <- file.path("~", "data") %>% path.expand
base_dir <- file.path("~", "temperature-france") %>% path.expand
output_dir <- file.path(base_dir, "data")
dir.create(output_dir, showWarnings = FALSE)
setwd(output_dir)

print("Loading MODIS reference grid")
system.time(
  grid <- file.path(base_dir, "grids", "modis_grid.rds") %>% readRDS
)
object_size(grid)
#    user  system elapsed
#   1.344   0.028   1.372
# 55.2 MB

epsg_2154 <- proj4string(grid)

print("Loading 1 km square buffers")
system.time(
  buffers <- file.path(base_dir, "buffers", "modis_square_1km.rds") %>% readRDS
)
object_size(buffers)
#   user  system elapsed
# 21.612   0.460  22.096
# 1.22 GB

print("Extracting IGN elevation data")
ign_dir <- file.path(data_dir, "ign", "BDALTI", "france")
system.time(
  ign_dem <- file.path(ign_dir, "BDALTIV2_25M_FXX_MNT_LAMB93_IGN69.tif") %>% raster
)
object_size(ign_dem)
#    user  system elapsed
#   0.036   0.000   0.053
# 10.5 kB

# Project the buffers to match the data
print("  Projecting buffers")
system.time(
  buffers_proj <- spTransform(buffers, proj4string(ign_dem))
)
object_size(buffers_proj)
#    user  system elapsed
# 227.984   2.100 230.316
# 1.22 GB

rm(buffers)

print("  Loading data with velox")
system.time(
  vx <- velox(ign_dem)
)
object_size(vx)
#    user  system elapsed
# 199.544  33.668 248.503
# 12.3 GB

print("  Extracting elevation")
system.time(
  grid$elevation <- vx$extract(buffers_proj, fun = mean)
)
object_size(grid)
# ... a very long time ...

# Save the result and clear memory
path <- file.path(output_dir, "modis_1km_ign_elevation.rds")
paste("  Saving to", path) %>% print
saveRDS(grid@data, path)
