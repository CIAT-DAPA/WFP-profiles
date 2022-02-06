# -------------------------------------------------- #
# Climate Risk Profiles -- CMIP6 data: GCM downscaling
# A. Ghosh & H. Achicanoy
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

options(warn = -1, scipen = 999)

suppressMessages(library(pacman))
suppressMessages(pacman::p_load(fuzzyjoin,terra,tidyverse,raster,sf,data.table,fst))
if (packageVersion("terra") < "1.1.0"){
  warning("terra version should be at least 1.1.0, \ninstall the the most updated version remotes::install_github('rspatial/terra')")
}

# supporting file
readRast <- function(f, sdate, edate){
  # as of terra_1.1-7, it is having problem with correctly reading time band from the netcdf files
  r <- rast(f)
  tm <- as.POSIXct(r@ptr$time, origin = "1970-01-01")
  # which bands are within the wmo baseline
  k <- which(tm >= sdate & tm <= edate)
  if(sum(k)>0){
    # subset those bands
    rs <- terra::subset(r, k)
    names(rs) <-  as.Date(tm[k])
    return(rs)
  }
}

checkCorruptFST <- function(f){
  
  file_damage <- FALSE
  
  tryCatch({ result <- fst::read_fst(f) }, error = function(e) {file_damage <<- TRUE})
  # expand for other kind type of files
  
  if(file_damage){
    cat("deleting", basename(f), "\n")
    unlink(f)
    flush.console()
  }
}

# check incomplete files
checkCorruptTIF <- function(f){
  
  file_damage <- FALSE
  
  tryCatch({ result <- raster::stack(f) }, error = function(e) {file_damage <<- TRUE})
  # expand for other kind type of files
  
  if(file_damage){
    cat("deleting", basename(f), "\n")
    unlink(f)
    flush.console()
  }else{
    cat("keeping", basename(f), "\n") 
  }
}

rotateGCM <- function(f, sdate, edate, interimdir, overwrite = FALSE){
  
  # GCM raster prep 
  # which ranges fall within start and end year

  # save intermediate rotated files
  rout <- basename(f)
  # unique file identifier 
  rout <- unique(sapply(strsplit(rout, "_gn_|_gr_|_gr1_"), "[", 1)) 
  
  # check for date range in the data
  rr <- readRast(f, sdate, edate)
  rcrs <- crs(rr)
  
  if(rcrs==""|is.na(rcrs)){
    crs(rr) <- "+proj=longlat +datum=WGS84 +no_defs"
  }
  
  dates <- names(rr)
  dtrange <- range(dates)
  rout <- paste0("rotated_", rout, "-", paste(dtrange, collapse = "_"), ".tif")
  routf <- file.path(interimdir, rout)
  
  if(file.exists(routf)){
    # might need to change the file size 
    if(file.size(routf) == 0){
      unlink(routf)
    }
    # if the TIF file is corrupted delete
    checkCorruptTIF(routf)
  }
  
  if(!file.exists(routf)){
    rot <- try(terra::rotate(rr, filename = routf, wopt= list(gdal=c("COMPRESS=LZW")), overwrite = overwrite), silent = FALSE)
    saveRDS(dates, gsub(".tif","_dates.rds",routf))
  }
  
  return(routf)
}  

# Function to get daily data from a single file
getGCMdailyTable <- function(i, setup, root, odir, ref, ff, overwrite = FALSE){
  
  pars <- setup[i,]
  iso <- pars$iso; var <- pars$var; model <- pars$model; experiment <- pars$exp; 
  sdate <- pars$sdate; edate <- pars$edate;
  
  cat("Processing", iso, var, model, experiment,"\n")
  
  # search files
  f <- grep(paste(var,model,experiment,sep = "_.*"), ff, value = TRUE)
  if(length(f) == 0){return(NULL)}
  
  # Where to intermediate results
  interimdir <- file.path(root, "climate/interim/rotated/CMIP6/daily")
  dir.create(interimdir, FALSE, TRUE)
  
  # Where to save results
  outdir <- file.path(odir, "climate/output/downscale/cmip6/daily", iso)
  dir.create(outdir, FALSE, TRUE)
  
  # input boundary
  vdir <- file.path(outdir, "input/vector")
  dir.create(vdir, FALSE, TRUE)
  
  # Country/zone boundary
  cmask <- file.path(vdir, paste0(iso, "_mask_chirps.tif"))
  
  if(!file.exists(cmask)){
    shp <- raster::getData("GADM", country = iso, level = 0, path = vdir)
    shp <- vect(shp)
    riso <- terra::crop(ref, shp, snap = "out")
    riso <- terra::mask(riso, shp, filename = cmask, wopt= list(gdal=c("COMPRESS=LZW")))
  } else {
    riso <- rast(cmask)
  }
  
  # GCM raster prep ############################################################
  # rotate
  routf <- rotateGCM(f, sdate, edate, interimdir, overwrite = FALSE)
  rot <- rast(routf)
  dates <- readRDS(gsub(".tif","_dates.rds",routf))  
  
  # resample to reference raster --- most time taking part
  soutf <- file.path(outdir, paste0(iso, "_" ,basename(routf)))
  
  #################################################################################################################
  # finally the fst files
  foutf <- gsub(".tif", ".fst", soutf)
  if(file.exists(foutf)){
    checkCorruptFST(foutf)
    }
  
  if(!file.exists(foutf)){
    # pixel center coordinates from CHIRPS
    cc <- as.data.frame(riso, xy=TRUE)
    dd <- extract(rot, cc[, c("x", "y")], cells=TRUE, xy=TRUE)
    dd$ID <- NULL
    # convert from wide to long with dataframe
    ddl <- melt(setDT(dd), id.vars = c("x","y", "cell"), value.name = var, variable = "date")
    write_fst(ddl, path = foutf)
  }
  cat("finished", basename(foutf), "\n")
  return(NULL)
}

##############################################################################################################
# Extractions setup
# iso <- c("BDI","HTI","GIN","GNB","MMR","NPL","NER","PAK","SOM","TZA")
iso <- c("TZA", "ETH", "SEN", "KEN", "PAK")
setup <- data.frame(expand.grid(iso = iso,
                                var = c('pr','tas','tasmax','tasmin'),
                                model = c("ACCESS-ESM1-5","EC-Earth3-Veg","INM-CM5-0","MPI-ESM1-2-HR","MRI-ESM2-0"),
                                exp = c('historical','ssp585_1','ssp585_2'), stringsAsFactors = FALSE))

setup$sdate <- NA
setup$edate <- NA
setup$sdate[setup$exp == 'historical'] <- "1995-01-01"
setup$edate[setup$exp == 'historical'] <- "2014-12-31"
setup$sdate[setup$exp == 'ssp585_1'] <- "2021-01-01"
setup$edate[setup$exp == 'ssp585_1'] <- "2040-12-31"
setup$sdate[setup$exp == 'ssp585_2'] <- "2041-01-01"
setup$edate[setup$exp == 'ssp585_2'] <- "2060-12-31"
setup$exp <- gsub('_1|_2','',setup$exp)


# Input parameters
# root <- "~/data"
# gcmdir <- paste0(root,"/input/climate/CMIP6/daily")
# root <- "/cluster01/workspace/AICCRA/Data"
root <- "/cluster01/workspace/common"
gcmdir <- file.path(root,"climate/cmip6/daily")
ff     <- list.files(gcmdir, pattern = ".nc$", full.names = TRUE)


# CHIRPS reference raster 
churl <- "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_dekad/tifs/chirps-v2.0.1981.01.1.tif.gz"
odir <- "/cluster01/workspace/AICCRA/Data"
vdir <- file.path(odir, "input/vector")
dir.create(vdir, FALSE, TRUE)
refile <- file.path(vdir, basename(churl))
ref <- gsub(".gz","",refile)
if(!file.exists(ref)){
  download.file(churl, dest = refile)
  R.utils::gunzip(refile)
  ref <- rast(ref)
} else {
  ref <- rast(ref)
}

# run all in parallel
rr <- parallel::mclapply(1:nrow(setup), getGCMdailyTable, setup, root, odir, ref, ff, overwrite = FALSE,
                         mc.preschedule = FALSE, mc.cores = 10)

# test 1
# getGCMdailyTable(6, setup, root, odir, ref, ff, overwrite = FALSE)

library(future.apply)
availableCores()
plan(multiprocess, workers = 10)
future_lapply(1:nrow(setup), getGCMdailyTable, setupx, root, odir, ref, ff, overwrite = FALSE, future.seed = TRUE)

