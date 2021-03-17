# -------------------------------------------------- #
# Climate Risk Profiles -- CMIP6 data: GCM downscaling
# A. Ghosh & H. Achicanoy
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

options(warn = -1, scipen = 999)

suppressMessages(library(pacman))
suppressMessages(pacman::p_load(DescTools,fuzzyjoin,terra,tidyverse,raster,sf,data.table,fst))
if (packageVersion("terra") < "1.1.0"){
  warning("terra version should be at least 1.1.0, \ninstall the the most updated version remotes::install_github('rspatial/terra')")
}

# STEP 1: rotate all the files and save the rotated ones 
rotateGCM <- function(i, idx, r, f, root, overwrite = FALSE){
  d <- idx[i,]
  # input files
  gcmdir <- paste0(root,"/input/climate/CMIP6/daily")
  flocal <- file.path(gcmdir, basename(d$file_url))
  
  # check if the file is relevant with the date range
  dateok <- dateCheck(d)
  
  # Where to save results
  outdir <- file.path(root, "interim/rotated/CMIP6/daily")
  dir.create(outdir, FALSE, TRUE)
  
  # if relevant
  if(dateok){
    ofile <- paste0(outdir, "/", "rotated_", flocal)
    ofile <- gsub(".nc$", ".tif", ofile)
    if(!file.exists(ofile)|overwrite){
      r <- rotate(r, e, filename = ofile, wopt= list(gdal=c("COMPRESS=LZW"))
    }
  }

}

dateCheck <- function(f, sdate = "1995-01-01", edate = "2060-12-31"){
  # filter files that donot contain the date range we want
  sdate <- as.Date(sdate)
  edate <- as.Date(edate)
  
  # which files to ignore
  drange <- c(sdate, edate)
  fdrange <- as.Date(c(f$file_start_date, f$file_end_date))
  dcheck <- fdrange  %overlaps% drange
  return(dcheck)          
}

root <- "~/data"

ff     <- list.files(gcmdir, pattern = ".nc$", recursive = TRUE, full.names = TRUE)
meta <- read.csv("data/cmip6_index.csv", stringsAsFactors = FALSE)


smeta <- meta[k,] 

isol <- c("BDI","HTI","GIN","GNB","MMR","NPL","NER","PAK","SOM","TZA")
isoGCM <- function(i, smeta, ff, iso, root){
  f <- smeta[i, ]
  fr <- ff[grep(basename(f$file_url), basename(ff))]
  r <- rast(fr)
  tm <- seq.Date(as.Date(f$datetime_start), as.Date(f$datetime_end), "day") 
  # which bands are within the wmo baseline
  k <- which(tm >= sdate & tm <= edate)
  # subset those bands
  r <- subset(r, k)
  # rotate
  r <- rotate(r)
}


# vdir <- file.path(root, "input/vector")
# dir.create(vdir, FALSE, TRUE)
# 
# # Country/zone boundary
# bext <- paste0(vdir, "/buffer_ext_", iso, ".rds")
# if(!file.exists(bext)){
#   shp <- raster::getData("GADM", country = iso, level = 0, path = vdir)
#   shpb <- buffer(shp, 0.1)
#   e <- terra::ext(shpb)
#   saveRDS(e, bext)
# } else {
#   e <- readRDS(bext)
# }


# k <- sapply(1:nrow(meta), 
#             function(i, meta, drange){x <- meta[i,]; as.Date(c(x$datetime_start, x$datetime_end)) %overlaps% drange},
#             meta, drange)

# Function to get daily data from a single file
getGCMdailyTable <- function(i, setup, ff, ref, root){
  pars <- setup[i,]
  iso <- pars$iso; var <- pars$var; model <- pars$model; experiment <- pars$exp; 
  sdate <- pars$sdate; edate <- pars$edate;
  
  cat("Processing", iso, var, model, experiment,"\n")
  
  # Where to save results
  outdir <- file.path(root, "interim/downscale/CMIP6", iso)
  dir.create(outdir, FALSE, TRUE)
  
  vdir <- file.path(root, "input/vector")
  dir.create(vdir, FALSE, TRUE)
  
  # Country/zone boundary
  bext <- paste0(vdir, "/buffer_ext_", iso, ".rds")
  if(!file.exists(bext)){
    shp <- raster::getData("GADM", country = iso, level = 0, path = vdir)
    shpb <- buffer(shp, 0.1)
    e <- terra::ext(shpb)
    saveRDS(e, bext)
  } else {
    e <- readRDS(bext)
  }
  
  # Crop reference raster
  riso <- terra::crop(x = ref, y = e)
  
  # search files
  spat <- paste0(var,"*",model,"*", experiment, "*")
  f <- ff[grep(glob2rx(spat), basename(ff))]
  
  # GCM raster prep ############################################################
  # which ranges fall within start and end year
  r <- terra::rast(f)
  # manage and convert to epoch time
  tm <- as.POSIXct(r@ptr$time, origin = "1970-01-01")
  # which bands are within the wmo baseline
  k <- which(tm >= sdate & tm <= edate)
  # subset those bands
  r <- terra::subset(r, k)
  # rotate
  r <- terra::rotate(r)
  # crop to extent
  rx <- terra::crop(r, e)
  # corresponding time; for now save as unix, convert to epoch later
  stm <- r@ptr$time
  
  # study area focus ###########################################################
  # resample to reference raster --- most time taking part
  ofile <- file.path(outdir, paste(iso,model,experiment,var,sdate,edate, sep = "_"))
  rx <- terra::resample(rx, riso)
  rx <- terra::mask(rx, mask = riso, filename = paste0(ofile, ".tif"), overwrite = TRUE)
  
  # Is this memory safe? get cell values
  dd <- terra::as.data.frame(rx, xy = TRUE)
  names(dd) <- c("x", "y", stm)
  
  # convert from wide to long with dataframe, safer way?
  ddl <- melt(setDT(dd), id.vars = c("x","y"), value.name = var, variable = "date")
  
  # add cellnumbers for using with join later
  xy <- as.matrix(ddl[,c("x","y")])
  ddl <- data.frame(id = cellFromXY(rx, xy), ddl, stringsAsFactors = FALSE)
  write_fst(ddl, path = paste0(ofile, "_interim.fst"))
  return(NULL)
  
}

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

# Extractions setup
iso <- c("BDI","HTI","GIN","GNB","MMR","NPL","NER","PAK","SOM","TZA")
setup <- data.frame(expand.grid(iso = iso,
                                var = c('pr','tas','tasmax','tasmin'),
                                model = c("BCC-CSM2-MR","CESM2","INM-CM5-0","MPI-ESM1-2-HR","MRI-ESM2-0"),
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
root <- "~/data"
gcmdir <- paste0(root,"/input/climate/CMIP6/daily")
ff     <- list.files(gcmdir, pattern = ".nc$", recursive = TRUE, full.names = TRUE)

# CHIRPS reference raster 
ref <- rast(crs = "epsg:4326", extent = ext(c(-180, 180, -50, 50)), resolution = 0.05, vals = 1)


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
