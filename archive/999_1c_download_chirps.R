u <- "https://data.chc.ucsb.edu/products/CHIRPS-2.0"
pname <- "global_daily"
format <- "tifs"
res <- "05"
years <- 1981:2020

downloadfiles <- function(h, datadir){
  dfile <- file.path(datadir, basename(h))
  if(!file.exists(dfile)){
    download.file(h, destfile = dfile, mode = "wb")
  }
}

datadir <- "data/input/climate/chirps"
dir.create(datadir, FALSE, TRUE)

for (year in years){
  dates <- seq.Date(as.Date(paste0(year, "-01-01")), as.Date(paste0(year, "-12-31")), by = "day")
  dates <- gsub("-", ".", dates)
  hurl <- file.path(u, pname, format, paste0("p", res), year, paste0("chirps-v2.0.", dates,".tif.gz"))
  parallel::mclapply(hurl, 
         function(h, datadir)tryCatch(downloadfiles(h, datadir), error=function(e) NULL), datadir,
         mc.preschedule = FALSE, mc.cores = 50)
}

