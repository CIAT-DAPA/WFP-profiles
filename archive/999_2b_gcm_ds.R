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
  r <- terra::rast(f)
  # manage and convert to epoch time
  tm <- as.POSIXct(r@ptr$time, origin = "1970-01-01")
  # which bands are within the wmo baseline
  k <- which(tm >= sdate & tm <= edate)
  if(sum(k)>0){
    # subset those bands
    r <- terra::subset(r, k)
    return(r)
  }
}

# Function to get daily data from a single file
getGCMdailyTable <- function(shp,iso,var,model,experiment,gcmdir,ff,sdate,edate,ref){
  
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
  shp <- getData("GADM", country = iso, level = 0, path = vdir)
  shp_sf <- shp %>% sf::st_as_sf()
  v <- sf::st_buffer(shp_sf, dist = 0.5) %>% sf::st_union(.) %>% sf::as_Spatial()
  e <- terra::ext(v)
  
  # Crop reference raster
  riso <- terra::crop(x = ref, y = e)
  
  # search files
  f <- grep(paste(var,model,experiment,sep = ".*"), ff, value = TRUE)
  
  # GCM raster prep ############################################################
  # which ranges fall within start and end year
  rr <- lapply(f, readRast, sdate, edate)
  rr[sapply(rr, is.null)] <- NULL
  r <- do.call("c", rr)
  
  # save intermediate rotated files
  rout <- basename(r@ptr$filenames)
  # split by gn
  rout <- unique(sapply(strsplit(rout, "_gn_"), "[", 1))
  # date range in the data
  dates <- as.POSIXct(r@ptr$time, origin = "1970-01-01")
  dtrange <- range(dates)
  rout <- paste0(rout, "-", paste(dtrange, collapse = "_"), ".tif")
  routf <- file.path(interimdir, rout)
  # time(r)  <- r@ptr$time
  
  if(!file.exists(ofile)|overwrite){
    rot <- try(rotate(r, filename = routf, wopt= list(gdal=c("COMPRESS=LZW")), overwrite = overwrite), silent = TRUE)
  } else {
    rot <- rast(routf)
  }
  
  # reproject outline to same coordinate system if needed?
  rx <- terra::crop(r, e)
  
  # study area focus ###########################################################
  # resample to reference raster --- most time taking part
  soutf <- file.path(outdir, paste0(iso, "_" ,basename(routf)))
  rx <- terra::resample(rx, riso, filename = soutf, wopt= list(gdal=c("COMPRESS=LZW")), overwrite = overwrite)
  
  # should we mask and save?
  # rx <- terra::mask(rx, mask = riso, filename = paste0(ofile, ".tif"), overwrite = TRUE)
  
  foutf <- gsub(".tif", ".fst", soutf)
  # Is this memory safe? get cell values
  dd <- terra::as.data.frame(rx, xy = TRUE)
  names(dd) <- c("x", "y", paste0(var, "_", dates))
  
  # convert from wide to long with dataframe, safer way?
  ddl <- melt(setDT(dd), id.vars = c("x","y"), value.name = var, variable = "date")
  
  # add cellnumbers for using with join later
  xy <- as.matrix(ddl[,c("x","y")])
  ddl <- data.frame(id = cellFromXY(rx, xy), ddl, stringsAsFactors = FALSE)
  write_fst(ddl, path = foutf)
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

####################################################################################################
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
gcmdir <- paste0(root,"/climate/CMIP6/daily")
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
