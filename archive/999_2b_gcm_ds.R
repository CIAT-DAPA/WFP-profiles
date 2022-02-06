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
  # r <- raster::stack(f)
  # # manage and convert to epoch time
  # tm <- as.Date(gsub("X","",names(r)), "%Y.%m.%d")
  r <- rast(f)
  tm <- as.POSIXct(r@ptr$time, origin = "1970-01-01")
  # which bands are within the wmo baseline
  k <- which(tm >= sdate & tm <= edate)
  if(sum(k)>0){
    # read the file as SpatRaster
    # rs <- terra::rast(f)
    # subset those bands
    rs <- terra::subset(r, k)
    names(rs) <-  as.Date(tm[k])
    return(rs)
  }
}


rotateGCM <- function(f, sdate, edate, interimdir, overwrite = FALSE){

  # GCM raster prep ############################################################
  # which ranges fall within start and end year
  # rr <- lapply(f, readRast, sdate, edate)
  # j <- sapply(rr, is.null)
  # rr[j] <- NULL
  # r <- do.call("c",rr)
  
  # save intermediate rotated files
  rout <- basename(f)
  # split by gn
  rout <- unique(sapply(strsplit(rout, "_gn_|_gr_|_gr1_"), "[", 1)) # check for 
  # date range in the data
  # dates <- as.POSIXct(r@ptr$time, origin = "1970-01-01")
  # dates <- as.Date(gsub("X","",names(r)), "%Y.%m.%d")
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
  }
  
  if(!file.exists(routf)){
    rot <- try(terra::rotate(rr, filename = routf, wopt= list(gdal=c("COMPRESS=LZW")), overwrite = overwrite), silent = FALSE)
    saveRDS(dates, gsub(".tif","_dates.rds",routf))
  }
  
  return(routf)
}  

# Function to get daily data from a single file
getGCMdailyTable <- function(i, setup, root, odir, ref, ff, bfile, overwrite = FALSE){
  
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
  vdir <- file.path(odir, "input/vector")
  dir.create(vdir, FALSE, TRUE)
  
  # Country/zone boundary
  cmask <- file.path(vdir, paste0(iso, "_mask_chirps.tif"))
  
  if(!file.exists(cmask)){
    # bfile <- "~/code/WFP-profiles/data/all_zones_countries.shp"
    
    # if(!missing(bfile)){
    #   bnd <- vect(bfile)
    #   shp <- bnd[bnd$iso3 == iso, ]
    #   
    #   tcrs <- "+proj=longlat +datum=WGS84 +no_defs"
    #   if(crs(shp, proj = TRUE)!= tcrs){
    #     shp <- project(shp, tcrs)
    #   }
    #   
    # } else {
      shp <- raster::getData("GADM", country = iso, level = 0, path = vdir)
      shp <- vect(shp)
    # }
    # shpb <- terra::buffer(shp, 10000) # Unit is meter if x has a longitude/latitude CRS
    # e <- terra::ext(shpb)
    # Crop reference raster
    riso <- terra::crop(ref, shp, snap = "out")
    riso <- terra::mask(riso, shp, filename = cmask, wopt= list(gdal=c("COMPRESS=LZW")), overwrite = TRUE)
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
  
  # if(!file.exists(soutf)){
  #   # reproject outline to same coordinate system if needed?
  #   rotsub <- terra::crop(rot, ext(riso))
  #   # disaggregate first
  #   rotsub <- terra::disaggregate(rotsub, fact =  round(res(rotsub)/res(riso)))
  #   # resample 
  #   rotsub <- terra::resample(rotsub, riso, filename = soutf, wopt= list(gdal=c("COMPRESS=LZW", "BIGTIFF=YES")), overwrite = overwrite)
  #   
  #   # ?implement a parallel resample
  # 
  #   # should we mask and save?
  #   # rotsub <- terra::mask(rotsub, mask = riso, filename = paste0(ofile, ".tif"), overwrite = TRUE)
  # } else {
  #   rotsub <- rast(soutf)
  # }
  
  #################################################################################################################
  # finally the fst files
  foutf <- gsub(".tif", ".fst", soutf)
  
  if(!file.exists(foutf)){
    # pixel center coordinates from CHIRPS
    cc <- as.data.frame(riso, xy=TRUE)
    dd <- extract(rot, cc[, c("x", "y")], cells=TRUE, xy=TRUE)
    dd$ID <- NULL
    # convert from wide to long with dataframe
    ddl <- melt(setDT(dd), id.vars = c("x","y", "cell"), value.name = var, variable = "date")
    write_fst(ddl, path = foutf)
  }
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



# is this causing the crash?
# ref <- rast(crs = "epsg:4326", extent = ext(c(-180, 180, -50, 50)), resolution = 0.05, vals = 1)

# rotate GCMs first?

# need to move to future apply

# prioritize HTI and BDI
# setupx <- setup[setup$iso == "NER",]
# setupx <- setup[setup$iso == "PAK",]

# setupx <- setup[setup$iso %in% c("PAK", "TZA", "SOM", "MMR"),]
# bfile <- "data/input/vector/all_zones_countries.shp"
rr <- parallel::mclapply(1:nrow(setup), getGCMdailyTable, setup, root, odir, ref, ff, overwrite = FALSE,
                          mc.preschedule = FALSE, mc.cores = 10)

# getGCMdailyTable(6, setup, root, odir, ref, ff, overwrite = FALSE)

library(future.apply)
availableCores()
plan(multiprocess, workers = 10)
future_lapply(1:nrow(setup), getGCMdailyTable, setupx, root, odir, ref, ff, overwrite = FALSE, future.seed = TRUE)


t1 <- Sys.time()
rr <- lapply(1:nrow(setupx), getGCMdailyTable, setupx, root, ref, ff, overwrite = FALSE)
Sys.time() - t1


# copy files to GCP
# gsutil -m cp -r /home/anighosh/data/output/downscale/CMIP6/daily/NER gs://cmip6results/data/output/downscale/CMIP6/daily/NER



# some cleanup
f1 <- list.files("~/data/interim/rotated/CMIP6/daily/", pattern = ".nc-",
                full.names = TRUE)
unlink(f1)


f2 <- list.files("~/data/output/downscale/CMIP6/daily/", pattern = ".nc-",
                 recursive = TRUE, full.names = TRUE)
unlink(f2)



# check status
odir <- "~/data/output/downscale/CMIP6/daily"
ff <- list.files(odir, pattern = ".tif$",recursive = TRUE, full.names = TRUE)
file.size(ff)*1e-9

# file check
odir <- "/cluster01/workspace/AICCRA/Data/climate/output/downscale/cmip6/daily"
ff <- list.files(odir, pattern = ".fst$", recursive = TRUE, full.names = TRUE)

getStatus <- function(iso){
  f <- grep(iso, ff, value = TRUE)
  x <- strsplit(basename(f), "_")
  var <- sapply(x, "[[", 3)
  model <- sapply(x, "[[", 5)
  date1 <- sapply(x,"[[", 7)
  date1 <- gsub("r1i1p1f1-", "", date1)
  date2 <- sapply(x,"[[", 8)
  date2 <- gsub(".fst", "", date2)
  data.frame(var, model, date1, date2)
}

xx <- list.files("/cluster01/workspace/common/climate/interim/rotated/CMIP6/daily", pattern = ".tif$", recursive=T)

getStatusGCM <- function(gcm){
  f <- grep(gcm, xx, value = TRUE)
  x <- strsplit(basename(f), "_")
  var <- sapply(x, "[[", 2)
  date1 <- sapply(x,"[[", 6)
  date1 <- gsub("r1i1p1f1-", "", date1)
  date2 <- sapply(x,"[[", 7)
  date2 <- gsub(".tif", "", date2)
  data.frame(var, date1, date2)
}


#country status
table(basename(dirname(ff)))

fs <- gsub(".tif", ".fst", f)
x <- fs[!file.exists(fs)]
x <- gsub(".fst", ".tif", x)


# clean up corrupted files

checkCorruptFiles <- function(f){
  
  file_damage <- FALSE
  
  tryCatch({ result <- fst::read_fst(f) }, error = function(e) {file_damage <<- TRUE})
  # expand for other kind type of files
  
  if(file_damage){
    cat("deleting", basename(f), "\n")
    unlink(f)
    flush.console()
  }
}
# x <- setup[setup$iso == "TZA" & setup$model == "MRI-ESM2-0" & setup$var == "pr",]
odir <- "~/data/output/downscale/CMIP6/daily"
ff <- list.files(odir, pattern = ".fst$", recursive = TRUE, full.names = TRUE)

iso <- "PAK"
ff <- grep(iso, ff, value = TRUE)
lapply(ff, checkCorruptFiles)
# parallel::mclapply(ff, checkCorruptFiles, mc.cores = 10, mc.preschedule = F)


# check incomplete files
deleteCorruptDownloads <- function(f){
  
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
ff <- list.files(downdir, pattern = ".nc",recursive = TRUE, full.names = TRUE)
lapply(ff, deleteCorruptDownloads)
 


###################################################################################################################
# Merge output tables
mergeGCMdailyTable <- function(iso, model, experiment, gcmdir, outdir, rref){
  cat("Processing", iso, model, experiment, "\n")
  dir.create(outdir, FALSE, TRUE)
  # search files
  dd <- list.files(file.path(gcmdir, "downscale", iso), pattern = "_interim.fst", recursive = TRUE, full.names = TRUE)
  if(experiment == 'ssp585'){
    d  <- grep(paste(model,experiment,sep = ".*"), dd, value = TRUE)
    c('2021','2041') %>%
      purrr::map(.f = function(yr){
        d <- grep(pattern = yr, x = d, value = TRUE)
        d <- d[-grep(pattern = '_tas_', x = d, fixed = T)]
        df <- lapply(d, function(x) data.table(read_fst(x)))
        df <- 1:length(df) %>%
          purrr::map(.f = function(i){
            if(i != 1){
              tbl <- df[[i]]
              tbl$x <- NULL
              tbl$y <- NULL
            } else {
              tbl <- df[[i]]
            }
            return(tbl)
          })
        # merge list of dataframes
        df <- Reduce(function(...) dplyr::inner_join(..., by = c("id","date")), df)
        # convert to epoch time
        df$date <- as.POSIXct(as.numeric(as.character(df$date)), origin="1970-01-01")
        df$date <- as.Date(df$date)
        names(df)[which(names(df) == 'pr')] <- 'prec'
        names(df)[which(names(df) == 'tasmax')] <- 'tmax'
        names(df)[which(names(df) == 'tasmin')] <- 'tmin'
        df$prec  <- df$prec*86400
        df$tmax  <- df$tmax-273.15
        df$tmin  <- df$tmin-273.15
        period   <- ifelse(yr == '2021', yes = '2021-2040', no = '2041-2060')
        out <- paste0(outdir,'/',iso,'/downscale/',period)
        dir.create(out, FALSE, TRUE)
        outfile <- paste0(out,'/',iso,'.fst')
        # Save output
        r <- raster::raster(rref)
        df$id <- raster::cellFromXY(object = r, xy = df[,c('x','y')])
        df <- df %>% dplyr::arrange(id,date)
        fst::write_fst(df, outfile)
      })
  } else {
    d  <- grep(paste(model,experiment,sep = ".*"), dd, value = TRUE)
    d <- d[-grep(pattern = '_tas_', x = d, fixed = T)]
    df <- lapply(d, function(x) data.table(read_fst(x)))
    df <- 1:length(df) %>%
      purrr::map(.f = function(i){
        if(i != 1){
          tbl <- df[[i]]
          tbl$x <- NULL
          tbl$y <- NULL
        } else {
          tbl <- df[[i]]
        }
        return(tbl)
      })
    # merge list of dataframes
    df <- Reduce(function(...) dplyr::inner_join(..., by = c("id","date")), df)
    # convert to epoch time
    df$date <- as.POSIXct(as.numeric(as.character(df$date)), origin="1970-01-01")
    df$date <- as.Date(df$date)
    names(df)[which(names(df) == 'pr')] <- 'prec'
    names(df)[which(names(df) == 'tasmax')] <- 'tmax'
    names(df)[which(names(df) == 'tasmin')] <- 'tmin'
    df$prec  <- df$prec*86400
    df$tmax  <- df$tmax-273.15
    df$tmin  <- df$tmin-273.15
    out <- paste0(outdir,'/',iso,'/downscale/1995-2014')
    dir.create(out, FALSE, TRUE)
    outfile <- paste0(out,'/',iso,'.fst')
    # Save output
    r <- raster::raster(rref)
    df$id <- raster::cellFromXY(object = r, xy = df[,c('x','y')])
    df <- df %>% dplyr::arrange(id,date)
    fst::write_fst(df, outfile)
    cat('GCM data save successfully\n')
  }
}

####################################################################################################


setup <- setup[setup$model == 'INM-CM5-0',] # INM-CM5-0
1:nrow(setup) %>%
  purrr::map(.f = function(i){
    t1 <- Sys.time()
    getGCMdailyTable(shp        = shp,
                     iso        = setup$iso[i],
                     var        = setup$var[i],
                     model      = setup$model[i],
                     experiment = setup$exp[i],
                     gcmdir     = gcmdir,
                     ff         = ff,
                     sdate      = setup$sdate[i],
                     edate      = setup$edate[i],
                     ref        = ref)
    cat("time to create table for", setup$iso[i], setup$var[i], setup$model[i], setup$exp[i], Sys.time() - t1, "\n")
  })

model <- 'INM-CM5-0'
experiment <- 'historical'
gcmdir <- paste0(root,"/1.Data/climate/CMIP6")
outdir <- paste0(root,'/1.Data/future_data/',model)
rref <- "//catalogue/BaseLineDataCluster01/observed/gridded_products/chirps/daily/chirps-v2.0.2020.01.01.tif"
mergeGCMdailyTable(iso, model, experiment, gcmdir, outdir, rref)
experiment <- 'ssp585'
mergeGCMdailyTable(iso, model, experiment, gcmdir, outdir, rref)


# parallel resample?
# create index of bands 
# k <- seq(1, nlyr(rx), ncores)
# k <- c(k, nlyr(rx))
# 
# parResample <- function(j, k, rx, riso){
#   sb <- k[j-1]:k[j]
#   rxb <- terra::subset(rxb, sb)
#   rxs <- terra::resample(rx, riso)
#   time(rxs) <- time(rxb)
#   return(rxs)
# }
# 
# d <- list()
# for(j in 2:length(k)){
#   d[[j]] <- k[j-1]:k[j]
# }