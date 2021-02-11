library(terra)
if (packageVersion("terra") < "1.1.0"){
  warning("terra version should be at least 1.1.0, \ninstall the the most updated version remotes::install_github('rspatial/terra')")
}

# list all files donwloaded for this project

gcmdir <- "~/data/"
ff <- list.files(gcmdir, pattern = ".nc$", recursive = TRUE, full.names = TRUE)

# process each combination for specific country
experiments <- c("historical", "ssp585")
vars <- c("pr","tas","tasmax","tasmin")
models <- c("BCC-CSM2-MR","GFDL-ESM4","INM-CM5-0","MPI-ESM1-2-HR","MRI-ESM2-0")
syear <- "1981-01-01"
eyear <- "2010-12-31"

# test case
var <- vars[3]
model <- models[3]
experiment <- experiments[1]


# search files
f <- grep(paste(model,experiment,var,sep = ".*"), ff, value = TRUE)

# which ranges fall within start and end year
r <- rast(f)
# get epoch time
tm <- r@ptr$time
# convert epoch time
tm <- as.POSIXct(tm, origin="1970-01-01")
# which bands are within the wmo baseline
k <- which(tm >= syear & tm <= eyear)
# subset those bands
r <- subset(r, k)

# crop by country boundary
v <- vect(raster::getData("GADM", country = "KEN", level = 0, path = tempdir()))
e <- ext(v)*1.25

rx <- crop(r, e)
plot(rx[[1]])
plot(v, add = TRUE)

# make a smaller subset to test
rx <- rx[[1:10]]
pp <- as.points(rx)
pp <- as.data.frame(as(pp, "Spatial"))
names(rd) <- paste0(var, '_', as.POSIXct(rx@ptr$time, origin="1970-01-01") )
