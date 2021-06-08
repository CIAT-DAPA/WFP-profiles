# creating cog with file structures
library(terra)

zip2cog <- function(f, cogdir){
  # r <- rast(paste0("/vsigzip/", f))
  tfile <- file.path(tdir, gsub(".gz","",basename(f)))
  R.utils::gunzip(f, destname = tfile)
  r <- rast(tfile)
  dates <- gsub("chirps-v2.0.", "", names(r))
  dates <- as.Date(gsub("\\.", "-", dates))
  # time(r) <- dates
  year <- format(dates, format="%Y")
  cat("Processing", year, "\n")
  odir <- file.path(cogdir, year)
  dir.create(odir, FALSE, TRUE)
  cofile <- file.path(odir, gsub(".gz","",basename(f)))
  writeRaster(r, cofile, overwrite=TRUE, wopt= list(gdal=c("COMPRESS=LZW", "of=COG")))
  rm(tfile)
}

datadir <-  "~/data/input/climate/chirps"
tdir <- "~/test/chirps/tif"
dir.create(tdir, F, T)

coverage <- "global"
interval <- "daily"
format <- "cogs"
resolution <- "05"
cogdir <- file.path("~/test/products/CHIRPS-2.0", paste0(coverage, "_", interval), format, paste0("p", resolution))
dir.create(cogdir, F, T)

ff <- list.files(datadir, pattern = ".gz$", full.names = TRUE)

parallel::mclapply(ff, zip2cog, cogdir, mc.preschedule = F, mc.cores = 50)

# copy to google cloud storage
# gsutil -m cp -r /home/anighosh/test/products/ gs://cogtest

library(terra)

getCHIRPS <- function(startdate, enddate){
  # setup file names
  seqdate <- seq.Date(as.Date(startdate), as.Date(enddate), by = "day")
  years <- format(seqdate, format="%Y")
  dates <- gsub("-","\\.",seqdate)
  fnames <- file.path(years, paste0("chirps-v2.0.", dates, ".tif"))
  
  # folder structure
  coverage <- "global"
  interval <- "daily"
  format <- "cogs"
  resolution <- "05"
  u <- file.path("https://storage.googleapis.com/cogtest/products/CHIRPS-2.0", 
                 paste0(coverage, "_", interval), format, paste0("p", resolution), fnames)
  u1 <- file.path("/vsicurl", u)
  r <- rast(u1)
  time(r) <- seqdate
  return(r)
}

# one full year!
rr <- getCHIRPS("2000-01-01", "2000-01-31")
# boundary for Burundi
v <- vect(raster::getData("GADM", country = "BDI", level = 0, path = tempdir()))
# crop for burundi
rv <- crop(rr, v)
# plot to check result
plot(rv[[1]])
plot(v, add = T)
