# WFP Climate Risk Project
# =----------------------
# Reading observed data. 
# A. Esquivel - H. Achicanoy - C.Saavedra 
# =----------------------

# R options
options(warn = -1, scipen = 999)

# Load libraries
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse, terra, gtools, future, furrr, lubridate, raster, tmap, fst, sf))


# Parameters 
ISO3 <- 'GIN'
country <- 'Guinee'

# Paths
OSys <<- Sys.info()[1]
root <<- switch(OSys,
                'Linux'   = '/dapadfs/workspace_cluster_14/WFP_ClimateRiskPr',
                'Windows' = '//CATALOGUE/Workspace14/WFP_ClimateRiskPr')

# =---------------------------------------------------
# Creando el CRD 
# layer_chirps <- raster("//catalogue/BaseLineDataCluster01/observed/gridded_products/chirps/daily/chirps-v2.0.1981.01.01.tif")
# Como crear el id. 
# spg <- layer_chirps %>% rasterToPoints() %>% 
#   as_tibble() %>% mutate(id = 1:nrow(.)) %>% dplyr::select(id, x, y)
# 
# coordinates(spg) <- ~ x + y
# gridded(spg) <- TRUE
# 
# # do a raster, with raster data. 
# raster_id <- raster(spg)
# 
# world_map <- shapefile('//dapadfs/workspace_cluster_8/climateriskprofiles/data/shps/all_country/all_countries.shp')
# crs(raster_id) = crs(world_map)
# 
# ext <- extent(world_map)
# rr <- raster(ext, res=0.5)
# rr <- rasterize(world_map, rr, field= world_map$OBJECTID)
# rr1 <- crop(rr, raster_id)
# 
# otro_r <- resample(x = rr, y = raster_id)
# values(otro_r) <- round(values(otro_r), 0)
# stack_special <- stack(otro_r, raster_id)
# 
# probando <- rasterToPoints(stack_special) %>% as_tibble()  
# tabla_final <- probando %>% mutate(layer = round(layer, 0)) %>%
#   dplyr::full_join(tibble(layer = world_map$OBJECTID, ISO3 = world_map$ISO3), .) %>% 
#   mutate(layer = ifelse(is.na(layer), 0 , layer), 
#          ISO3 = ifelse(is.na(ISO3), 'sea', ISO3))

# write_csv(x = tabla_final, file = "//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/crd_world.csv")

# ggplot() + geom_tile(data = filter(tabla_final, layer >0), 
#             aes(x = x, y = y, fill = layer )) +
#   coord_fixed() +  theme_bw()
# =---------------------------------------------------
# Reading id for all world. 
tabla_final <- read_csv("//dapadfs/workspace_cluster_14/WFP_ClimateRiskPr/1.Data/crd_world.csv")
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
  # ext <<- ext
  
  
  # =-----------------------------------------------------------------
  
  # =--- read a reference layer from chirps data. . 
  layer_r <- terra::rast("//catalogue/BaseLineDataCluster01/observed/gridded_products/chirps/daily/chirps-v2.0.1981.01.01.tif") 
  
  # crop id raster data by ERA5 extent. 
  layer_id <- terra::crop(raster_id, ext) %>%
    terra::mask(. , ext) # %>% 
    # terra::as.data.frame(xy = TRUE) %>% 
    # as_tibble()
  
  layer_r <- terra::crop(x = layer_r, y = ext) %>%
    terra::mask(. , ext)
  
  gc(reset = TRUE)
  # =-----------------------------------------------------------------
  

  # Create read ERA5 
  # =---------------------
  # =- ERA5 data management.

  vlst <- c('rh', 'srad','tmax', 'tmean', 'tmin', 'wind')
  
  # This code list all the files in order by year to facilitate 
  # the processing
  unzip_nc <- function(vlst, year){
    files_n <- list.files(path  = paste0(root,'/1.Data/climate/AgERA5/', vlst),
                          pattern    = year %>% as.character,
                          full.names = T) 
    
    obj <- 0
    for(i in 1:length(files_n) ){obj[i] <- unzip(files_n[i])}
 
  return(obj)}
  # =- 
  var_cl <- function(var, year){
    gn_data <- unzip_nc(year = year, vlst = var)
    
    r <- terra::rast(gn_data) %>% terra::crop(. ,  ext) %>%
      terra::mask(. , ext)
    
    if(var == 'srad'){
      r <- r/1000000 
    }else if(var %in% c('tmax', 'tmean', 'tmin')){
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
    
    r_data <- r_data %>% 
      mutate(id = as.integer(id))  %>% 
      pivot_longer(cols = contains('d_'), names_to = 'date', values_to = var) %>% 
      mutate(date = str_remove(date, 'd_') %>% lubridate::ymd()) %>% 
      dplyr::select(x, y, id, date, var)
    
    
    gc(reset = TRUE)
    return(r_data)}

  
  # tictoc::tic()
  ERA5_final <- tibble(vlst = c('rh','srad','tmax', 'tmean','tmin','wind')) %>% 
    mutate(data = purrr::map(.x = vlst, .f = var_cl,  year = year))
  # tictoc::toc() # 9.95 min --- c('rh','srad','tmax', 'tmean','tmin','wind')
  
  gc(reset = TRUE)
  
  ERA5_final <- ERA5_final %>% 
    dplyr::group_split(vlst) %>% 
    purrr::map(.f = function(x){dplyr::select(x, -vlst) %>% unnest}) %>% 
    reduce(full_join)
   
  
  
  # =-----------------------------------------------
  # =- CHIPS management data. 
  
  # d <- '//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/ERA5/chirps-v2.0.2020.01.01.tif'
  # Search all files name for an objective year. 
  files_chirps <-list.files(path = glue::glue("//catalogue/BaseLineDataCluster01/observed/gridded_products/chirps/daily"), 
                            pattern = as.character(year), full.names = TRUE)
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
    mutate(id = as.integer(id))  %>% 
    dplyr::na_if(-9999) %>%
    pivot_longer(cols = contains('d_'), names_to = 'date', values_to = 'prec') %>% 
    mutate(date = str_remove(date, 'd_') %>% lubridate::ymd()) %>% 
    dplyr::select(x, y, id, date, prec)
  
  
  gc(reset = TRUE)
  # =------------------------------------------------
  # Join ERA5 and CHIRPS data, remove all NA's from prec. 
  climate_t <- full_join(ERA5_final, raw_ly) %>% drop_na(prec)
  
  # Where save the file. 
  foder_save <- glue::glue('//dapadfs/workspace_cluster_14/WFP_ClimateRiskPr/1.Data/observed_data/{iso3}/year')
  if(dir.exists(foder_save) == FALSE){dir.create(foder_save, recursive = TRUE)}else{print('ok')}
  # Save a file by one year
  fst::write_fst(x = climate_t, path = glue::glue('{foder_save}/climate_{year}_mod.fst'))
  # Return climate data. 
  return(climate_t)}



# =------------------------- 
# Pruebas para correr 

# Read all shp
# tab <- tibble(iso3 = ISO3, country = country, year = 1982)
# tictoc::tic()
# one_y <- observed_data(tab = tab, regions = FALSE)
# tictoc::toc() # 15.02 min

# =-----------------------------------------------------------?.??
# tictoc::tic()
ob_run <- tibble(iso3 = ISO3, country = country, year = 1981:2000) %>%
  mutate(row = 1:nrow(.)) %>%
  nest(-row) %>%
  mutate(climate = purrr::map(.x = data, observed_data))
# tictoc::toc()


# if the process had an error and ran in parts, run this line. 
# Please, once the last process has run completely for all years. 
# ob_run <- tibble(iso3 = ISO3, country = country, year = 1981:2019) %>%
#   mutate(row = 1:nrow(.)) %>%
#   nest(-row) %>%
#   mutate(climate = purrr::map(.x = data, .f = function(z){
#     x <- z$iso3; y <- z$year
#     path <- glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/observed_data/{x}/year/climate_{y}_mod.fst')
#     cl_d <- fst::fst(path) %>% as_tibble()
#     return(cl_d)}))


ob_run <- dplyr::select(ob_run, - row) %>% unnest() 
fst::write_fst(x = ob_run, path = glue::glue('//dapadfs/workspace_cluster_14/WFP_ClimateRiskPr/1.Data/observed_data/{ISO3}/{ISO3}.fst') )