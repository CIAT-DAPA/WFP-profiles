# -------------------------------------------------- #
# Climate Risk Profiles -- CMIP6 data: GCM bias-correction performed by Delta method
# H. Achicanoy, C. Saavedra, A. Ghosh
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# R options
options(warn = -1, scipen = 999)

# Load libraries
suppressMessages(if(!require("pacman")) install.packages("pacman"))
suppressMessages(pacman::p_load(tidyverse, raster, terra, fields, geosphere, tidyfst, tidyft, parallel, future, future.apply, furrr))

OSys <- Sys.info()[1]
root <- switch(OSys,
               'Linux'   = '/dapadfs/workspace_cluster_14/WFP_ClimateRiskPr',
               'Windows' = '//CATALOGUE/Workspace14/WFP_ClimateRiskPr')

# Bias correction function using Delta method
BC_Delta <- function(iso     = iso,
                     gcm     = gcm,
                     fut_bc  = fut_bc,
                     period  = period){
  
  # Get monthly anomalies function
  get_anomaly <- function(m){
    
    cat(paste0('>>> Month: ', m, '\n'))
    cat('1. Calculating long-term monthly averages\n')
    his_years <- unique(lubridate::year(as.Date(names(his_gcm))))
    his_stack <- terra::rast(lapply(X = his_years, FUN = function(hyear){
      if(var %in% c('tmax','tmin')){
        avg <- mean(his_gcm[[lubridate::year(as.Date(names(his_gcm))) == hyear & lubridate::month(as.Date(names(his_gcm))) == m]])
      } else {
        avg <- sum(his_gcm[[lubridate::year(as.Date(names(his_gcm))) == hyear & lubridate::month(as.Date(names(his_gcm))) == m]])
      }
      return(avg)
    }))
    avg_his <- mean(his_stack)
    fut_years <- unique(lubridate::year(as.Date(names(fut_gcm))))
    fut_stack <- terra::rast(lapply(X = fut_years, FUN = function(fyear){
      if(var %in% c('tmax','tmin')){
        avg <- mean(fut_gcm[[lubridate::year(as.Date(names(fut_gcm))) == fyear & lubridate::month(as.Date(names(fut_gcm))) == m]])
      } else {
        avg <- sum(fut_gcm[[lubridate::year(as.Date(names(fut_gcm))) == fyear & lubridate::month(as.Date(names(fut_gcm))) == m]])
      }
      return(avg)
    }))
    avg_fut <- mean(fut_stack)
    
    cat('2. Calculating long-term monthly anomalies\n')
    if(var %in% c('tmax','tmin')){
      anom <- avg_fut - avg_his
    } else {
      anom <- (avg_fut - avg_his)/avg_his
    }
    
    crds <- anom %>%
      terra::as.data.frame(xy = T)
    
    empty <- anom
    terra::values(empty) <- NA
    
    anomalies_values <- unique(crds[,'mean'])
    
    cat('3. Interpolating long-term monthly anomalies\n')
    crds2 <- 1:length(anomalies_values) %>%
      purrr::map(.f = function(i){
        pnt <- crds[crds$mean == anomalies_values[i],] # c('x','y')
        rst <- tryCatch(expr = {
          raster::rasterFromXYZ(xyz = pnt)
        }, error = function(e){
          return(NULL)
        })
        if(!is.null(rst)){
          rw  <- trunc(nrow(rst)/2)
          cl  <- trunc(ncol(rst)/2)
          cell <- raster::cellFromRowCol(rst, rw, cl)
          centroids <- base::as.data.frame(raster::xyFromCell(rst, cell))
          centroids$Anomaly <- anomalies_values[i]
        } else{
          centroids <- NULL
        }
        return(centroids)
      }) %>%
      dplyr::bind_rows()
    tps  <- fields::Tps(x = crds2[,c('x','y')], Y = crds2[,'Anomaly'])
    intp <- raster::interpolate(raster::raster(empty), tps)
    intp <- terra::rast(intp) %>% terra::mask(mask = anom)
    return(intp)
    
  }
  
  # Load raster template
  tmpl <- raster::raster(paste0(root,'/1.Data/chirps-v2.0.2020.01.01.tif'))
  
  cat('# ----------------------------- #\n')
  cat('  Load historical observations during baseline period 1995-2014.\n')
  cat('# ----------------------------- #\n')
  his_obs <- paste0(root,'/1.Data/observed_data/',iso,'/',iso,'.fst') %>%
    tidyfst::parse_fst(path = .) %>%
    base::as.data.frame() %>%
    dplyr::filter(year %in% 1995:2014)
  
  cat('# ----------------------------- #\n')
  cat('  Bias correcting Tmax variable.\n')
  cat('# ----------------------------- #\n')
  
  # Reading GCM data
  his_gcm <- terra::rast(paste0(root,'/1.Data/climate/CMIP6/daily/',iso,'/',iso,'_rotated_tasmax_day_',gcm,'_historical_r1i1p1f1-1995-01-01_2014-12-31.tif'))
  his_gcm <- his_gcm - 273.15
  fut_gcm <- terra::rast(paste0(root,'/1.Data/climate/CMIP6/daily/',iso,'/',iso,'_rotated_tasmax_day_',gcm,'_ssp585_r1i1p1f1-',strsplit(x = period, split = '-')[[1]][1],'-01-01_',strsplit(x = period, split = '-')[[1]][2],'-12-31.tif'))
  fut_gcm <- fut_gcm - 273.15
  
  # Define variable of interest
  var <- 'tmax'
  
  # Compute monthly anomalies
  monthly_anomalies <- 1:12 %>%
    purrr::map(.f = get_anomaly)
  monthly_anomalies <- terra::rast(monthly_anomalies)
  names(monthly_anomalies) <- paste0('m_',1:12)
  
  # Get historical tmax values
  tmax_obs <- his_obs %>%
    dplyr::select(x,y,date,tmax) %>%
    tidyr::pivot_wider(names_from = 'date', values_from = 'tmax')
  
  # Transform tabular data into raster
  tmax_rst <- terra::rast(x = base::as.matrix(tmax_obs), type = 'xyz')
  tmax_rst <- terra::crop(x = tmax_rst, y = terra::ext(monthly_anomalies))
  crs(tmax_rst) <- crs(monthly_anomalies)
  
  # Sum monthly anomalies to daily data
  tmax_fut <- 1:12 %>%
    purrr::map(.f = function(m){
      m_fut <- tmax_rst[[lubridate::month(as.Date(names(tmax_rst))) == m]] + monthly_anomalies[[m]]
      return(m_fut)
    }) %>% terra::rast()
  # Sort by dates
  tmax_fut <- tmax_fut[[as.character(sort(as.Date(names(tmax_fut))))]]
  
  tmax_fut_tbl <- tmax_fut %>% terra::as.data.frame(xy = T)
  tmax_fut_tbl <- tmax_fut_tbl %>%
    tidyr::pivot_longer(cols      = 3:ncol(tmax_fut_tbl),
                        names_to  = 'date',
                        values_to = 'tmax')
  tmax_fut_tbl <- tmax_fut_tbl %>%
    dplyr::mutate(date = date %>%
                    base::gsub('X','',.,fixed=T) %>%
                    base::gsub('.','-',.,fixed=T) %>%
                    base::as.Date())
  
  tmax_fut_tbl$id <- raster::cellFromXY(object = tmpl, xy = as.matrix(tmax_fut_tbl[,c('x','y')]))
  
  cat('# ----------------------------- #\n')
  cat('  Bias correcting Tmin variable.\n')
  cat('# ----------------------------- #\n')
  
  # Reading GCM data
  his_gcm <- terra::rast(paste0(root,'/1.Data/climate/CMIP6/daily/',iso,'/',iso,'_rotated_tasmin_day_',gcm,'_historical_r1i1p1f1-1995-01-01_2014-12-31.tif'))
  his_gcm <- his_gcm - 273.15
  fut_gcm <- terra::rast(paste0(root,'/1.Data/climate/CMIP6/daily/',iso,'/',iso,'_rotated_tasmin_day_',gcm,'_ssp585_r1i1p1f1-',strsplit(x = period, split = '-')[[1]][1],'-01-01_',strsplit(x = period, split = '-')[[1]][2],'-12-31.tif'))
  fut_gcm <- fut_gcm - 273.15
  
  # Define variable of interest
  var <- 'tmin'
  
  # Compute monthly anomalies
  monthly_anomalies <- 1:12 %>%
    purrr::map(.f = get_anomaly)
  monthly_anomalies <- terra::rast(monthly_anomalies)
  names(monthly_anomalies) <- paste0('m_',1:12)
  
  # Get historical tmin values
  tmin_obs <- his_obs %>%
    dplyr::select(x,y,date,tmin) %>%
    tidyr::pivot_wider(names_from = 'date', values_from = 'tmin')
  
  # Transform tabular data into raster
  tmin_rst <- terra::rast(x = base::as.matrix(tmin_obs), type = 'xyz')
  tmin_rst <- terra::crop(x = tmin_rst, y = terra::ext(monthly_anomalies))
  crs(tmin_rst) <- crs(monthly_anomalies)
  
  # Sum monthly anomalies to daily data
  tmin_fut <- 1:12 %>%
    purrr::map(.f = function(m){
      m_fut <- tmin_rst[[lubridate::month(as.Date(names(tmin_rst))) == m]] + monthly_anomalies[[m]]
      return(m_fut)
    }) %>% terra::rast()
  # Sort by dates
  tmin_fut <- tmin_fut[[as.character(sort(as.Date(names(tmin_fut))))]]
  
  tmin_fut_tbl <- tmin_fut %>% terra::as.data.frame(xy = T)
  tmin_fut_tbl <- tmin_fut_tbl %>%
    tidyr::pivot_longer(cols      = 3:ncol(tmin_fut_tbl),
                        names_to  = 'date',
                        values_to = 'tmin')
  tmin_fut_tbl <- tmin_fut_tbl %>%
    dplyr::mutate(date = date %>%
                    base::gsub('X','',.,fixed=T) %>%
                    base::gsub('.','-',.,fixed=T) %>%
                    base::as.Date())
  
  tmin_fut_tbl$id <- raster::cellFromXY(object = tmpl, xy = as.matrix(tmin_fut_tbl[,c('x','y')]))
  
  cat('# ----------------------------- #\n')
  cat('  Bias correcting Prec variable.\n')
  cat('# ----------------------------- #\n')
  
  # Reading GCM data
  his_gcm <- terra::rast(paste0(root,'/1.Data/climate/CMIP6/daily/',iso,'/',iso,'_rotated_pr_day_',gcm,'_historical_r1i1p1f1-1995-01-01_2014-12-31.tif'))
  his_gcm <- his_gcm * 86400
  fut_gcm <- terra::rast(paste0(root,'/1.Data/climate/CMIP6/daily/',iso,'/',iso,'_rotated_pr_day_',gcm,'_ssp585_r1i1p1f1-',strsplit(x = period, split = '-')[[1]][1],'-01-01_',strsplit(x = period, split = '-')[[1]][2],'-12-31.tif'))
  fut_gcm <- fut_gcm * 86400
  
  # Define variable of interest
  var <- 'prec'
  
  # Compute monthly anomalies
  monthly_anomalies <- 1:12 %>%
    purrr::map(.f = get_anomaly)
  monthly_anomalies <- terra::rast(monthly_anomalies)
  names(monthly_anomalies) <- paste0('m_',1:12)
  
  # Get historical tmin values
  prec_obs <- his_obs %>%
    dplyr::select(x,y,date,prec) %>%
    tidyr::pivot_wider(names_from = 'date', values_from = 'prec')
  
  # Transform tabular data into raster
  prec_rst <- terra::rast(x = base::as.matrix(prec_obs), type = 'xyz')
  prec_rst <- terra::crop(x = prec_rst, y = terra::ext(monthly_anomalies))
  crs(prec_rst) <- crs(monthly_anomalies)
  
  # Sum monthly anomalies to daily data
  prec_fut <- 1:12 %>%
    purrr::map(.f = function(m){
      m_fut <- prec_rst[[lubridate::month(as.Date(names(prec_rst))) == m]] + (1+monthly_anomalies[[m]])
      return(m_fut)
    }) %>% terra::rast()
  # Sort by dates
  prec_fut <- prec_fut[[as.character(sort(as.Date(names(prec_fut))))]]
  
  prec_fut_tbl <- prec_fut %>% terra::as.data.frame(xy = T)
  prec_fut_tbl <- prec_fut_tbl %>%
    tidyr::pivot_longer(cols      = 3:ncol(prec_fut_tbl),
                        names_to  = 'date',
                        values_to = 'prec')
  prec_fut_tbl <- prec_fut_tbl %>%
    dplyr::mutate(date = date %>%
                    base::gsub('X','',.,fixed=T) %>%
                    base::gsub('.','-',.,fixed=T) %>%
                    base::as.Date())
  
  prec_fut_tbl$id <- raster::cellFromXY(object = tmpl, xy = as.matrix(prec_fut_tbl[,c('x','y')]))
  
  if(identical(tmax_fut_tbl$id, tmin_fut_tbl$id)){
    corrected <- dplyr::bind_cols(tmax_fut_tbl,
                                  tmin_fut_tbl %>% dplyr::select(tmin),
                                  prec_fut_tbl %>% dplyr::select(prec))
  }
  
  fut_gcm_bc <- his_obs
  fut_gcm_bc$tmax <- fut_gcm_bc$tmin <- fut_gcm_bc$tmean <- fut_gcm_bc$prec <- NULL
  fut_gcm_bc <- dplyr::left_join(x = fut_gcm_bc, y = corrected %>% dplyr::select(id,date,tmax,tmin,prec), by = c('id','date'))
  
  fut_gcm_bc <- fut_gcm_bc %>%
    dplyr::mutate(tmean = (tmax + tmin)/2)
  
  hyears <- 1995:2014
  fyears <- as.numeric(strsplit(x = period, split = '-')[[1]][1]):as.numeric(strsplit(x = period, split = '-')[[1]][2])
  
  fut_gcm_bc$year[fut_gcm_bc$year %in% hyears] <- fyears[match(fut_gcm_bc$year, hyears, nomatch = 0)]
  fut_gcm_bc$date <-  as.Date(paste0(fut_gcm_bc$year,'-',substring(text = fut_gcm_bc$date, first = 5, last = nchar(fut_gcm_bc$date))))
  
  dir.create(dirname(fut_bc),FALSE,TRUE)
  tidyft::export_fst(fut_gcm_bc,fut_bc)
  
}

iso <- 'TZA'
gcm <- 'EC-Earth3-Veg'
period <- '2021-2040'
fut_bc <- paste0(root,'/1.Data/future_data/',gcm,'/',iso,'/bias_corrected/',period,'/',iso,'.fst')
BC_Delta(iso     = iso,
         gcm     = gcm,
         fut_bc  = fut_bc,
         period  = period)
