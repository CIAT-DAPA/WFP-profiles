library(R.utils)
library(terra)
datadir <- "/home/anighosh/data/chirps"
dir.create(datadir, FALSE, TRUE)

u <- "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_pentad/tifs/chirps-v2.0.1981.01.1.tif.gz"
download.file(u, destfile = file.path(datadir, basename(u)), mode = "wb")
gunzip(file.path(datadir, basename(u)), remove=FALSE)

r <- rast(file.path(datadir, "chirps-v2.0.1981.01.1.tif"))


x <- rast(crs = "epsg:4326", extent = ext(c(-180, 180, -50, 50)), resolution = 0.05)
