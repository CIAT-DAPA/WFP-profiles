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

root <- "//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr"

# Function to get daily data from a single file
getGCMdailyTable <- function(shp,iso,var,model,experiment,gcmdir,ff,sdate,edate,ref){
  
  cat("Processing", iso, var, model, experiment,"\n")
  
  # Where to save results
  outdir <- file.path(gcmdir, "downscale", iso)
  dir.create(outdir, FALSE, TRUE)
  
  # Country/zone boundary
  shp <- raster::shapefile(shp)
  shp_sf <- shp %>% sf::st_as_sf()
  v <- sf::st_buffer(shp_sf, dist = 0.5) %>% sf::st_union(.) %>% sf::as_Spatial()
  e <- terra::ext(v)
  # Crop reference raster
  riso <- terra::crop(x = ref, y = e)
  
  # search files
  f <- grep(paste(model,experiment,var,sep = ".*"), ff, value = TRUE)
  
  # GCM raster prep ############################################################
  # which ranges fall within start and end year
  r <- terra::rast(f)
  # manage and convert to epoch time
  tm <- as.POSIXct(r@ptr$time, origin = "1970-01-01")
  # which bands are within the wmo baseline
  k <- which(tm >= sdate & tm <= edate)
  # subset those bands
  r <- terra::subset(r, k)
  r <- terra::rotate(r)
  # reproject outline to same coordinate system if needed
  # vp <- project(v, crs(r))
  # ep <- ext(vp)*1.25
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
        })
        df <- 1:length(df) %>%
          purrr::map(.f = function(i){
            if(i != 1){
              tbl <- df[[i]]
              tbl$x <- NULL
              tbl$y <- NULL
            } else {
              tbl <- df[[i]]
              tbl$id   <- as.numeric(tbl$id)
              tbl$date <- as.Date(tbl$date)
            }
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
        fst::write_fst(df, outfile)
      })
  } else {
    d  <- grep(paste(model,experiment,sep = ".*"), dd, value = TRUE)
    if(length(grep(pattern = '_tas_', x = d, fixed = T)) > 0){d <- d[-grep(pattern = '_tas_', x = d, fixed = T)]}
    df <- lapply(d, function(x){
      tb <- data.table(read_fst(x))
      if(names(tb)[1] == 'cell_id'){names(tb)[1] <- 'id'}
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
    fst::write_fst(df, outfile)
    cat('GCM data save successfully\n')
  }
}

# Extractions setup
iso <- 'TZA'
setup <- data.frame(expand.grid(iso = iso, # 'HTI'
                                var = c('pr','tas','tasmax','tasmin'),
                                model = c("GFDL-ESM4","INM-CM5-0","MPI-ESM1-2-HR","MRI-ESM2-0"), # "BCC-CSM2-MR"
                                exp = c('historical','ssp585_1','ssp585_2')
))
setup$sdate <- NA
setup$edate <- NA
setup$sdate[setup$exp == 'historical'] <- "1995-01-01"
setup$edate[setup$exp == 'historical'] <- "2014-12-31"
setup$sdate[setup$exp == 'ssp585_1'] <- "2021-01-01"
setup$edate[setup$exp == 'ssp585_1'] <- "2040-12-31"
setup$sdate[setup$exp == 'ssp585_2'] <- "2041-01-01"
setup$edate[setup$exp == 'ssp585_2'] <- "2060-12-31"
setup$exp <- gsub('_1|_2','',setup$exp)

setup$iso <- as.character(setup$iso)
setup$var <- as.character(setup$var)
setup$model <- as.character(setup$model)

# Input parameters
gcmdir <- paste0(root,"/1.Data/climate/CMIP6")
ff     <- list.files(gcmdir, pattern = ".nc$", recursive = TRUE, full.names = TRUE)
shp    <- paste0(root,"/1.Data/shps/tanzania/tza_gadm/Tanzania_GADM1.shp")

# CHIRPS reference raster 
ref <- terra::rast("//catalogue/BaseLineDataCluster01/observed/gridded_products/chirps/daily/chirps-v2.0.2020.01.01.tif")
ref[ref == -9999] <- NA

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
