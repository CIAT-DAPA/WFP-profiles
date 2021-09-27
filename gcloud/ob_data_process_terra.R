# WFP Climate Risk Project
# =----------------------
# Reading observed data. 
# A. Esquivel - H. Achicanoy - C.Saavedra 
# =----------------------

# R options
options(warn = -1, scipen = 999)

# Load libraries
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse, tidyft, terra, gtools, future, furrr, lubridate, raster, tmap, fst, sf))


# Parameters 
ISO3 <- 'SOM'
country <- 'Somalia'

# Paths
OSys <- Sys.info()[1]
root <<- switch(OSys,
                'Linux'   = '/home/anighosh/data/WFP_ClimateRiskPr',
                'Windows' = '//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr')
setwd(root)

# Reading id for all world. 
tabla_final <- paste0(root,"/1.Data/crd_world.fst") %>% tidyft::parse_fst() %>% base::as.data.frame()
tabla_final <<- tabla_final

spg <- tabla_final %>% dplyr::select(id, x, y)
coordinates(spg) <- ~ x + y
gridded(spg) <- TRUE
# # do a raster, with raster data. 
raster_id <- raster(spg) %>% terra::rast(.)

raster_id <<- raster_id
# =---------------------------------------------------
# Input example. 
observed_data <- function(tab, regions = FALSE){
  iso3 <- tab$iso3; country <- tab$country ; year <- tab$year
  
  if(regions == TRUE){
    shp <<- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_regions/",tolower(iso3),"_regions.shp"))
  }else{
    shp <<- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_gadm/",country,"_GADM1.shp"))
  }
  
  shp_sf <- shp %>% sf::st_as_sf() #%>% st_union(.)
  ext <- sf::st_buffer(shp_sf, dist = 0.5) %>% 
    sf::st_union(.) %>% sf::as_Spatial()
  ext <- ext %>% terra::vect(.)
  
  # =--- read a reference layer from chirps data. . 
  layer_r <- terra::rast(paste0(root,"/1.Data/chirps-v2.0.2020.01.01.tif"))
  
  # crop id raster data by ERA5 extent. 
  layer_id <- terra::crop(raster_id, ext) %>%
    terra::mask(. , ext)
  
  layer_r <- terra::crop(x = layer_r, y = ext) %>%
    terra::mask(. , ext)
  
  gc(reset = TRUE)
  
  # Create read ERA5 
  # =---------------------
  # =- ERA5 data management.
  
  # vlst <- c('rh', 'srad','tmax', 'tmean', 'tmin', 'wind')
  vlst <- c('2m_relative_humidity','solar_radiation','2m_temperature-24_hour_maximum','2m_temperature-24_hour_mean','2m_temperature-24_hour_minimum','10m_wind_speed')
  
  # This code list all the files in order by year to facilitate 
  # the processing
  unzip_nc <- function(vlst, year){
    files_n <- list.files(path = '/home/anighosh/data/input/climate/AgERA5', pattern = vlst, full.names = T) %>%
      grep(pattern = as.character(year), x = ., value = T)
    
    obj <- 0
    for(i in 1:length(files_n) ){obj[i] <- unzip(files_n[i])}
    
    return(obj)}
  # =- 
  var_cl <- function(var, year){
    gn_data <- unzip_nc(year = year, vlst = var)
    
    r <- terra::rast(gn_data) %>% terra::crop(. ,  ext) %>%
      terra::mask(. , ext)
    
    if(var == 'solar_radiation'){
      r <- r/1000000 
    }else if(var %in% c('2m_temperature-24_hour_maximum', '2m_temperature-24_hour_mean', '2m_temperature-24_hour_minimum')){
      r <- r -273.15
    }
    
    r <- terra::resample(x = r, y = layer_r)
    r <- terra::rast(list(layer_id, r))
    r_data <- terra::as.data.frame(r, xy = T) %>% as_tibble() 
    
    # pattern <- dplyr::case_when(var == 'srad' ~ 'Solar_Radiation_Flux_', 
    #                             var == 'tmax' ~ 'Temperature_Air_2m_Max_24h_',
    #                             var == 'tmean' ~ 'Temperature_Air_2m_Mean_24h_',
    #                             var == 'tmin' ~ 'Temperature_Air_2m_Min_24h_', 
    #                             var == 'rh' ~ 'Relative_Humidity_2m_12h_', 
    #                             var == 'wind' ~ 'Wind_Speed_10m_Mean_')
    
    d_s <- seq(ymd(paste0(year, '-01-01')),ymd(paste0(year, '-12-31')),by='day')
    names(r_data)[-(1:3)] <- glue::glue('d_{d_s}')
    
    if(var == '2m_relative_humidity'){var2 <- 'rh'}
    if(var == 'solar_radiation'){var2 <- 'srad'}
    if(var == '2m_temperature-24_hour_maximum'){var2 <- 'tmax'}
    if(var == '2m_temperature-24_hour_mean'){var2 <- 'tmean'}
    if(var == '2m_temperature-24_hour_minimum'){var2 <- 'tmin'}
    if(var == '10m_wind_speed'){var2 <- 'wind'}
    
    r_data <- r_data %>% 
      dplyr::mutate(id = as.integer(id))  %>% 
      tidyr::pivot_longer(cols = contains('d_'), names_to = 'date', values_to = var2) %>% 
      dplyr::mutate(date = str_remove(date, 'd_') %>% lubridate::ymd()) %>% 
      dplyr::select(x, y, id, date, var2)
    
    gc(reset = TRUE)
    return(r_data)}
  
  # tictoc::tic()
  ERA5_final <- tibble(vlst = c('2m_relative_humidity','solar_radiation','2m_temperature-24_hour_maximum','2m_temperature-24_hour_mean','2m_temperature-24_hour_minimum','10m_wind_speed')) %>% # c('rh','srad','tmax', 'tmean','tmin','wind')
    dplyr::mutate(data = purrr::map(.x = vlst, .f = var_cl,  year = year))
  # tictoc::toc() # 9.95 min --- c('rh','srad','tmax', 'tmean','tmin','wind')
  
  gc(reset = TRUE)
  
  ERA5_final <- ERA5_final %>% 
    dplyr::group_split(vlst) %>% 
    purrr::map(.f = function(x){dplyr::select(x, -vlst) %>% tidyr::unnest()}) %>% 
    purrr::reduce(dplyr::full_join)
  
  # =-----------------------------------------------
  # =- CHIPS management data. 
  
  # d <- '//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/ERA5/chirps-v2.0.2020.01.01.tif'
  # Search all files name for an objective year. 
  files_chirps <-list.files(path = glue::glue("/home/anighosh/data/input/climate/chirps"), 
                            pattern = as.character(year), full.names = TRUE)
  files_chirps %>%
    purrr::map(.f = function(f){
      if(!file.exists(paste0(root,'/',basename(gsub(sprintf("[.]%s$", "gz"), "", f, ignore.case = TRUE))))){
        R.utils::decompressFile(filename = f,
                                destname = paste0(root,'/',basename(gsub(sprintf("[.]%s$", "gz"), "", f, ignore.case = TRUE))),
                                ext      = 'gz',
                                remove   = F,
                                FUN      = gzfile)
      }
    })
  files_chirps <- list.files(path = root, pattern = 'chirps', full.names = T) %>%
    grep(pattern = as.character(year), x = ., value = T)
  # files_chirps <- c(d, files_chirps)
  # Only use the tif. 
  chirps_ <- files_chirps[str_detect(files_chirps, '.tif')]
  
  # Create a stack with CHIRPS data and crop using ERA5 extent. 
  # tictoc::tic()
  chirps_ <- terra::rast(chirps_) %>% terra::crop(., ext) %>%
    terra::mask(. , ext)
  # tictoc::toc()
  
  chirps_ <- terra::rast(list(layer_id, chirps_))
  dates_c <- seq(ymd(paste0(year, '-01-01')),ymd(paste0(year, '-12-31')),by='day')
  
  raw_ly <- chirps_ %>% terra::as.data.frame(xy = TRUE) %>% as_tibble()
  
  
  names(raw_ly)[-(1:3)] <- glue::glue('d_{dates_c}')
  
  raw_ly <- raw_ly %>%
    dplyr::mutate(id = as.integer(id))  %>% 
    dplyr::na_if(-9999) %>%
    tidyr::pivot_longer(cols = contains('d_'), names_to = 'date', values_to = 'prec') %>% 
    dplyr::mutate(date = str_remove(date, 'd_') %>% lubridate::ymd()) %>% 
    dplyr::select(x, y, id, date, prec)
  
  
  gc(reset = TRUE)
  # =------------------------------------------------
  # Join ERA5 and CHIRPS data, remove all NA's from prec. 
  climate_t <- dplyr::full_join(ERA5_final, raw_ly) %>% tidyr::drop_na(prec)
  
  # Where save the file. 
  foder_save <- glue::glue('{root}/1.Data/observed_data/{iso3}/year')
  if(dir.exists(foder_save) == FALSE){dir.create(foder_save, recursive = TRUE)}else{print('ok')}
  # Save a file by one year
  fst::write_fst(x = climate_t, path = glue::glue('{foder_save}/climate_{year}_mod.fst'))
  # Return climate data. 
  return(climate_t)
  list.files(path = root, pattern = '.tif$', full.names = T) %>% purrr::map(.f = function(x) file.remove(x))
  list.files(path = root, pattern = '.nc$', full.names = T) %>% purrr::map(.f = function(x) file.remove(x))
}

# =------------------------- 
# Pruebas para correr 

# Read all shp
# tab <- tibble(iso3 = ISO3, country = country, year = 1982)
# tictoc::tic()
# one_y <- observed_data(tab = tab, regions = FALSE)
# tictoc::toc() # 15.02 min

# =-----------------------------------------------------------?.??
# tictoc::tic()
ob_run <- tibble(iso3 = ISO3, country = country, year = 1981:2019) %>%
  dplyr::mutate(row = 1:nrow(.)) %>%
  tidyr::nest(-row) %>%
  dplyr::mutate(climate = purrr::map(.x = data, observed_data))
# tictoc::toc()

ob_run <- dplyr::select(ob_run, - row) %>% unnest() 
fst::write_fst(x = ob_run, path = glue::glue('{root}/1.Data/observed_data/{ISO3}/{ISO3}.fst'))
