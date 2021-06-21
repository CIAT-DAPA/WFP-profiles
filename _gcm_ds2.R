# -------------------------------------------------- #
# Climate Risk Profiles -- CMIP6 data: GCM downscaling
# A. Ghosh & H. Achicanoy
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

options(warn = -1, scipen = 999)

suppressMessages(library(pacman))
suppressMessages(pacman::p_load(fuzzyjoin,terra,tidyverse,raster,sf,data.table,fst,tidyft,future.apply))
if (packageVersion("terra") < "1.1.0"){
  warning("terra version should be at least 1.1.0, \ninstall the the most updated version remotes::install_github('rspatial/terra')")
}

# supporting file
readRast <- function(f, sdate, edate){
  # as of terra_1.1-7, it is having problem with correctly reading time band from the netcdf files
  f <- grep(pattern = paste0(sdate,'_',edate), x = f, value = T)
  r <- raster::stack(f)
  # manage and convert to epoch time
  tm <- seq(from = as.Date(sdate), to = as.Date(edate), by = 1) # tm <- as.Date(gsub("X","",names(r)), "%Y.%m.%d")
  if(length(grep(pattern = 'INM-CM5-0', x = f)) > 0){
    df <- data.frame(date = tm)
    df <- df %>%
      dplyr::mutate(month = lubridate::month(date),
                    day   = lubridate::day(date)) %>%
      .[-which(.$month == 2 & .$day == 29),]
    tm <- df$date; rm(df)
  }
  # which bands are within the wmo baseline
  k <- which(tm >= sdate & tm <= edate)
  if(sum(k)>0){
    # read the file as SpatRaster
    rs <- terra::rast(f)
    # subset those bands
    rs <- terra::subset(rs, k)
    names(rs) <-  tm[k]
    return(rs)
  }
}

rotateGCM <- function(f, sdate, edate, interimdir, overwrite = FALSE){
  
  # GCM raster prep ############################################################
  # which ranges fall within start and end year
  rr <- lapply(f, readRast, sdate, edate)
  j <- sapply(rr, is.null)
  rr[j] <- NULL
  r <- do.call("c",rr)
  
  rcrs <- crs(r)
  
  if(rcrs==""|is.na(rcrs)){
    crs(r) <- "+proj=longlat +datum=WGS84 +no_defs"
  }
  
  # save intermediate rotated files
  rout <- basename(f)[!j]
  # split by gn
  rout <- unique(sapply(strsplit(rout, "_gn_|_gr_|_gr1_"), "[", 1)) # check for 
  # date range in the data
  # dates <- as.POSIXct(r@ptr$time, origin = "1970-01-01")
  # dates <- as.Date(gsub("X","",names(r)), "%Y.%m.%d")
  dates <- names(r)
  dtrange <- range(dates)
  # rout <- paste0("rotated_", rout, "-", paste(dtrange, collapse = "_"), ".tif")
  routf <- file.path(interimdir, rout)
  if(!file.exists(gsub(".tif","_dates.rds",routf))){
    saveRDS(dates, gsub(".tif","_dates.rds",routf))
  }
  
  # if(file.exists(routf)){
  #   # might need to change the file size 
  #   if(file.size(routf) == 0){
  #     unlink(routf)
  #   }
  # }
  
  # if(!file.exists(routf)){
  #   rot <- try(terra::rotate(r, filename = routf, wopt= list(gdal=c("COMPRESS=LZW")), overwrite = overwrite), silent = FALSE)
  #   saveRDS(dates, gsub(".tif","_dates.rds",routf))
  # }
  return(routf)
}

# Function to get daily data from a single file
getGCMdailyTable <- function(i, rgn_shp, setup, root, ref, ff, overwrite = FALSE){
  
  pars <- setup[i,]
  iso <- pars$iso; var <- pars$var; model <- pars$model; experiment <- pars$exp; 
  sdate <- pars$sdate; edate <- pars$edate;
  
  cat("Processing", iso, var, model, experiment,"\n")
  
  # Where to intermediate results
  interimdir <- file.path(root, "interim/rotated/CMIP6/daily")
  dir.create(interimdir, FALSE, TRUE)
  
  # Where to save results
  outdir <- file.path(root, "output/downscale/CMIP6/daily", iso)
  dir.create(outdir, FALSE, TRUE)
  
  # input boundary
  vdir <- file.path(root, "input/vector")
  dir.create(vdir, FALSE, TRUE)
  
  # Country/zone boundary
  cmask <- file.path(vdir, paste0(iso, "_mask_chirps.tif"))
  if(!file.exists(cmask)){
    shp <- raster::shapefile(rgn_shp)
    if(iso == 'NER'){shp <- spTransform(shp, raster::crs("+proj=longlat +datum=WGS84"))}
    shpb <- raster::buffer(shp, 0.05)
    e <- terra::ext(shpb)
    shpr <- terra::rasterize(x = terra::vect(shpb), y = ref) %>% terra::crop(x = ., y = e)
    # Crop reference raster
    riso <- terra::crop(x = ref, y = e) %>%
      terra::mask(x = ., mask = shpr, filename = cmask, wopt = list(gdal=c("COMPRESS=LZW")))
  } else {
    riso <- terra::rast(cmask)
  }
  
  # search files
  f <- grep(paste(var,model,experiment,sep = "_.*"), ff, value = TRUE)
  f <- grep(pattern = paste0(sdate,'_',edate), x = f, value = T)
  
  # GCM raster prep ############################################################
  # rotate
  routf <- rotateGCM(f, sdate, edate, interimdir, overwrite = FALSE)
  rot <- terra::rast(f)
  dates <- readRDS(gsub(".tif","_dates.rds",routf))
  
  # resample to reference raster --- most time taking part
  soutf <- file.path(outdir, paste0(iso, "_" ,basename(routf)))
  
  if(!file.exists(soutf)){
    # reproject outline to same coordinate system if needed?
    rotsub <- terra::crop(rot, terra::ext(riso))
    # disaggregate first
    rotsub <- terra::disaggregate(rotsub, fact = round(terra::res(rotsub)/terra::res(riso)))
    names(rotsub) <- dates
    # resample
    rotsub <- terra::resample(rotsub, riso) # filename = soutf, wopt= list(gdal=c("COMPRESS=LZW")), overwrite = overwrite
    
    # ?implement a parallel resample
    
    # should we mask and save?
    rotsub <- terra::mask(rotsub, mask = riso, filename = soutf, wopt = list(gdal=c("COMPRESS=LZW")), overwrite = overwrite)
  } else {
    rotsub <- terra::rast(soutf)
  }
  
  #################################################################################################################
  # finally the fst files
  foutf <- gsub(".tif", ".fst", soutf)
  
  if(!file.exists(foutf)){
    # Is this memory safe? get cell values
    dd <- terra::as.data.frame(rotsub, xy = TRUE)
    # names(dd) <- c("x", "y", dates)
    names(dd) <- c("x", "y", names(rotsub))
    
    # convert from wide to long with dataframe, safer way?
    ddl <- melt(setDT(dd), id.vars = c("x","y"), value.name = var, variable = "date")
    
    # add cellnumbers for using with join later
    xy <- as.matrix(ddl[,c("x","y")])
    ddl <- data.frame(cell_id = cellFromXY(rotsub, xy), ddl, stringsAsFactors = FALSE)
    tidyft::export_fst(x = ddl, path = foutf)
  }
  return(NULL)
}

###########################################################################################################################################
# Merge output tables
mergeGCMdailyTable <- function(iso, model, experiment, gcmdir, outdir, rref){
  cat("Processing", iso, model, experiment, "\n")
  dir.create(outdir, FALSE, TRUE)
  # search files
  dd <- list.files(file.path(gcmdir, "downscale", iso), pattern = "*.fst", recursive = TRUE, full.names = TRUE)
  if(experiment == 'ssp585'){
    d  <- grep(paste(model,experiment,sep = ".*"), dd, value = TRUE)
    c('2021','2041') %>%
      purrr::map(.f = function(yr){
        d <- grep(pattern = yr, x = d, value = TRUE)
        if(length(grep(pattern = '_tas_', x = d, fixed = T)) > 0){d <- d[-grep(pattern = '_tas_', x = d, fixed = T)]}
        df <- lapply(d, function(x){
          tb <- data.table(read_fst(x))
          if(names(tb)[1] == 'cell_id'){names(tb)[1] <- 'id'}
          return(tb)
        })
        df <- 1:length(df) %>%
          purrr::map(.f = function(i){
            if(i != 1){
              tbl <- df[[i]]
              tbl$x <- NULL
              tbl$y <- NULL
            } else {
              tbl <- df[[i]]
            }
            tbl$id   <- as.character(tbl$id)
            tbl$date <- as.Date(tbl$date)
            return(tbl)
          })
        # merge list of dataframes
        df <- Reduce(function(...) dplyr::inner_join(..., by = c("id","date")), df)
        # convert to epoch time
        # df$date <- as.POSIXct(as.numeric(as.character(df$date)), origin="1970-01-01")
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
        tidyft::export_fst(df, outfile)
      })
  } else {
    d  <- grep(paste(model,experiment,sep = ".*"), dd, value = TRUE)
    if(length(grep(pattern = '_tas_', x = d, fixed = T)) > 0){d <- d[-grep(pattern = '_tas_', x = d, fixed = T)]}
    df <- lapply(d, function(x){
      tb <- data.table(read_fst(x))
      if(names(tb)[1] == 'cell_id'){names(tb)[1] <- 'id'}
      return(tb)
    })
    df <- 1:length(df) %>%
      purrr::map(.f = function(i){
        if(i != 1){
          tbl <- df[[i]]
          tbl$x <- NULL
          tbl$y <- NULL
        } else {
          tbl <- df[[i]]
        }
        tbl$id   <- as.character(tbl$id)
        tbl$date <- as.Date(tbl$date)
        return(tbl)
      })
    # merge list of dataframes
    df <- Reduce(function(...) dplyr::inner_join(..., by = c("id","date")), df)
    # convert to epoch time
    # df$date <- as.POSIXct(as.numeric(as.character(df$date)), origin="1970-01-01")
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
    tidyft::export_fst(df, outfile)
    cat('GCM data save successfully\n')
  }
}

##################################################################################################################
# Extractions setup
iso <- c("TZA") # "BDI","HTI","GIN","GNB","MMR","NPL","PAK","SOM","NER"
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
setup <- setup[setup$var != 'tas',]

#######################################################################################################
# cloud setup
# Input parameters
root <- "//dapadfs.cgiarad.org/workspace_cluster_14/WFP_ClimateRiskPr/1.Data/climate"
gcmdir <- paste0(root,"/interim/rotated/CMIP6/daily")
ff     <- list.files(gcmdir, pattern = ".tif$", full.names = TRUE)

# CHIRPS reference raster 
churl <- "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_dekad/tifs/chirps-v2.0.1981.01.1.tif.gz"
refile <- file.path(root, "input/vector", basename(churl))
ref <- gsub(".gz","",refile)
if(!file.exists(ref)){
  download.file(churl, dest = refile)
  R.utils::gunzip(refile)
  ref <- terra::rast(ref)
} else {
  ref <- terra::rast(ref)
}

# If there are in priority countries
country <- 'Tanzania'
iso     <- 'TZA'
setupx  <- setup[setup$iso == iso,]
rownames(setupx) <- 1:nrow(setupx)
rgn_shp <- paste0('//dapadfs.cgiarad.org/workspace_cluster_14/WFP_ClimateRiskPr/1.Data/shps/',tolower(country),'/',tolower(iso),'_regions/',tolower(iso),'_regions.shp')

library(future.apply)
availableCores()
plan(multiprocess, workers = 5, gc = TRUE)
future_lapply(1:nrow(setupx), getGCMdailyTable, rgn_shp, setupx, root, ref, ff, overwrite = FALSE, future.seed = TRUE)
future:::ClusterRegistry("stop")
gc(reset = T)

# #################################################################################################################
# # cali cluster setup
# # Input parameters
# gcmdir <- paste0(root,"/1.Data/climate/CMIP6")
# ff     <- list.files(gcmdir, pattern = ".nc$", recursive = TRUE, full.names = TRUE)
# shp    <- paste0(root,"/1.Data/shps/tanzania/tza_gadm/Tanzania_GADM1.shp")
# 
# # CHIRPS reference raster 
# ref <- terra::rast("//catalogue/BaseLineDataCluster01/observed/gridded_products/chirps/daily/chirps-v2.0.2020.01.01.tif")
# ref[ref == -9999] <- NA
# 
# setup <- setup[setup$model == 'INM-CM5-0',] # INM-CM5-0
# 1:nrow(setup) %>%
#   purrr::map(.f = function(i){
#     t1 <- Sys.time()
#     getGCMdailyTable(shp        = shp,
#                      iso        = setup$iso[i],
#                      var        = setup$var[i],
#                      model      = setup$model[i],
#                      experiment = setup$exp[i],
#                      gcmdir     = gcmdir,
#                      ff         = ff,
#                      sdate      = setup$sdate[i],
#                      edate      = setup$edate[i],
#                      ref        = ref)
#     cat("time to create table for", setup$iso[i], setup$var[i], setup$model[i], setup$exp[i], Sys.time() - t1, "\n")
#   })
# 
# model <- 'INM-CM5-0'
# experiment <- 'historical'
# gcmdir <- paste0(root,"/1.Data/climate/CMIP6")
# outdir <- paste0(root,'/1.Data/future_data/',model)
# rref <- "//catalogue/BaseLineDataCluster01/observed/gridded_products/chirps/daily/chirps-v2.0.2020.01.01.tif"
# mergeGCMdailyTable(iso, model, experiment, gcmdir, outdir, rref)
# experiment <- 'ssp585'
# mergeGCMdailyTable(iso, model, experiment, gcmdir, outdir, rref)
