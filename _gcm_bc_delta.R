# -------------------------------------------------- #
# Climate Risk Profiles -- CMIP6 data: GCM bias-correction performed by Delta method
# H. Achicanoy, C. Saavedra, A. Ghosh
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# R options
options(warn = -1, scipen = 999)
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_main_functions.R') # Main functions

# Load libraries
suppressMessages(if(!require("pacman")) install.packages("pacman"))
suppressMessages(pacman::p_load(tidyverse, raster, terra, fields, geosphere, tidyfst, parallel, future, future.apply, furrr))

OSys <- Sys.info()[1]
root <- switch(OSys,
               'Linux'   = '/dapadfs/workspace_cluster_14/WFP_ClimateRiskPr',
               'Windows' = '//CATALOGUE/Workspace14/WFP_ClimateRiskPr')

root <- '//CATALOGUE/Workspace14/WFP_ClimateRiskPr/1.Data/climate/CMIP6/daily/TZA'

# Bias correction function using Delta method
BC_Delta <- function(his_obs = his_obs,
                     his_gcm = his_gcm,
                     fut_gcm = fut_gcm,
                     his_bc  = his_bc,
                     fut_bc  = fut_bc,
                     period  = period,
                     ncores  = 1){
  
  
  
}

his_gcm <- terra::rast(paste0(root,'/TZA_rotated_tasmax_day_EC-Earth3-Veg_historical_r1i1p1f1-1995-01-01_2014-12-31.tif'))
fut_gcm <- terra::rast(paste0(root,'/TZA_rotated_tasmax_day_EC-Earth3-Veg_ssp585_r1i1p1f1-2021-01-01_2040-12-31.tif'))

# grep(pattern = 'tasmax', x = ...)
var <- 'tmax'

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

monthly_anomalies <- 1:12 %>%
  purrr::map(.f = get_anomaly)

if(OSys == 'Windows'){
  future::plan(cluster, workers = 12, gc = TRUE)
  monthly_anomalies <- 1:12 %>%
    furrr::future_map(.x = ., .f = get_anomaly)
  future:::ClusterRegistry("stop")
  gc(reset = T)
} else {
  if(OSys == 'Linux'){
    plan(multicore, workers = 12)
    index_by_pixel <- 1:12 %>%
      future.apply::future_lapply(X = ., FUN = get_anomaly)
    future:::ClusterRegistry("stop")
    gc(reset = T)
  }
}












his_obs <- '//catalogue/Workspace14/WFP_ClimateRiskPr/1.Data/observed_data/TZA/TZA.fst' %>%
  tidyfst::parse_fst(path = .) %>%
  base::as.data.frame() %>%
  dplyr::filter(year >= 2000)

his_obs$month <- lubridate::month(his_obs$date)

monthly_anomalies <- terra::rast(monthly_anomalies)
names(monthly_anomalies) <- paste0('m_',1:12)
anomalies_dfm <- terra::as.data.frame(x = monthly_anomalies, xy = T)

tmpl <- raster::raster('//catalogue/Workspace14/WFP_ClimateRiskPr/1.Data/chirps-v2.0.2020.01.01.tif')

anomalies_dfm$id <- raster::cellFromXY(object = tmpl, xy = base::as.data.frame(anomalies_dfm[,c('x','y')]))
anomalies_dfm_long <- anomalies_dfm %>%
  tidyr::pivot_longer(cols = m_1:m_12, names_to = 'month', values_to = 'Anomaly')
anomalies_dfm_long$month <- as.numeric(gsub(pattern = 'm_', replacement = '', x = anomalies_dfm_long$month))

his_obs2 <- his_obs
his_obs2 <- dplyr::left_join(x = his_obs2, y = anomalies_dfm_long %>% dplyr::select(id, month, Anomaly), by = c('id','month'))
his_obs2 <- his_obs2[complete.cases(his_obs2),]

id <- anomalies_dfm$id[1]

tbl1 <- his_obs[his_obs$month == 1 & his_obs$id == id,c('date','tmax')]
tbl1$tmax <- tbl1$tmax + anomalies_dfm$m_1[1]
tbl2 <- his_obs[his_obs$month == 2 & his_obs$id == id,c('date','tmax')]
tbl2$tmax <- tbl2$tmax + anomalies_dfm$m_2[1]
tbl3 <- his_obs[his_obs$month == 3 & his_obs$id == id,c('date','tmax')]
tbl3$tmax <- tbl3$tmax + anomalies_dfm$m_3[1]
tbl4 <- his_obs[his_obs$month == 4 & his_obs$id == id,c('date','tmax')]
tbl4$tmax <- tbl4$tmax + anomalies_dfm$m_4[1]
tbl5 <- his_obs[his_obs$month == 5 & his_obs$id == id,c('date','tmax')]
tbl5$tmax <- tbl5$tmax + anomalies_dfm$m_5[1]
tbl6 <- his_obs[his_obs$month == 6 & his_obs$id == id,c('date','tmax')]
tbl6$tmax <- tbl6$tmax + anomalies_dfm$m_6[1]
tbl7 <- his_obs[his_obs$month == 7 & his_obs$id == id,c('date','tmax')]
tbl7$tmax <- tbl7$tmax + anomalies_dfm$m_7[1]
tbl8 <- his_obs[his_obs$month == 8 & his_obs$id == id,c('date','tmax')]
tbl8$tmax <- tbl8$tmax + anomalies_dfm$m_8[1]
tbl9 <- his_obs[his_obs$month == 9 & his_obs$id == id,c('date','tmax')]
tbl9$tmax <- tbl9$tmax + anomalies_dfm$m_9[1]
tbl10 <- his_obs[his_obs$month == 10 & his_obs$id == id,c('date','tmax')]
tbl10$tmax <- tbl10$tmax + anomalies_dfm$m_10[1]
tbl11 <- his_obs[his_obs$month == 11 & his_obs$id == id,c('date','tmax')]
tbl11$tmax <- tbl11$tmax + anomalies_dfm$m_11[1]
tbl12 <- his_obs[his_obs$month == 12 & his_obs$id == id,c('date','tmax')]
tbl12$tmax <- tbl12$tmax + anomalies_dfm$m_12[1]

fut_res <- dplyr::bind_rows(tbl1,tbl2,tbl3,tbl4,tbl5,tbl6,
                 tbl7,tbl8,tbl9,tbl10,tbl11,tbl12)

fut_res <- fut_res %>% dplyr::mutate(date = as.Date(date)) %>% dplyr::arrange(date)
his_res <- his_obs %>% dplyr::filter(id == 7341014) %>% dplyr::select(date, tmax) %>% dplyr::mutate(date = as.Date(date)) %>% dplyr::arrange(date)

