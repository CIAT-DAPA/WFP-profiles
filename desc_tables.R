# rm(list = ls()); gc(reset = TRUE)

# WFP Climate Risk Project
# =----------------------
# Tables  
# A. Esquivel - H. Achicanoy - C.Saavedra 
# =----------------------

# R options
options(warn = -1, scipen = 999)

# Load libraries
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse, terra, gtools, future, furrr, lubridate, raster, terra,tmap, fst, sf))

# Paths
OSys <- Sys.info()[1]
root <<- switch(OSys,
                'Linux'   = '/home/jovyan/work/cglabs',
                'Windows' = '//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr')


# =-------------------------------------------------------
# =-------------------------------------
# Parameters 
iso3 <- 'BDI'
country <- 'Burundi'
seasons <- list(s1 =	c(2,3,4,5,6,7),
                s2 =	c(9,10,11,12,1,2))
# =-------------------------------------

# Reading the tables of future indices. 
to_do <<- readxl::read_excel('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/regions_ind.xlsx') %>% 
  filter(ISO3 == iso3) %>% 
  rename('Livehood_z' = 'Livelihood zones', 'NT_X'= "NT-X")


# Regions shp
regions_all <- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_regions/",tolower(iso3),"_regions.shp"))
regions_all <- regions_all %>%  sf::st_as_sf() %>% 
  group_by(region) %>% summarise() %>% 
  mutate(Short_Name = to_do$Short_Name)
regions_all <<- regions_all


# =---------------------------------------------------
# Functions. 
read_data_seasons <- function(country, iso3) {
  
  # Read all data complete. 
  past <- fst::fst( glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/past/{iso3}_indices.fst') )%>% 
    tibble::as_tibble() %>% mutate(time = 'Historic')
  
  gcm <- c('INM-CM5-0', 'ACCESS-ESM1-5', 'EC-Earth3-Veg', 'MPI-ESM1-2-HR', 'MRI-ESM2-0')
  
  future <- tibble( file = list.files(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/{gcm}/'), full.names =  TRUE, recursive = TRUE, pattern = '_indices') ) %>%
    dplyr::filter(!grepl('_monthly', file) & !grepl('_old', file)) %>% 
    mutate(shot_file = str_remove(file, pattern = glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/'))) %>% 
    mutate(data = purrr::map(.x = file, .f = function(x){x <- fst::fst(x) %>% tibble::as_tibble()})) %>%
    mutate(str_split(shot_file, '/') %>% 
             purrr::map(.f = function(x){tibble(gcm = x[1], time = x[3])}) %>% 
             bind_rows()) %>% 
    dplyr::select(gcm, time, data) %>% 
    unnest()
  
  future  <- future %>% dplyr::select(-gcm) %>% 
    group_by(time,id,x,y,season,year) %>%
    summarise_all(~mean(. , na.rm =  TRUE)) %>%
    mutate_at(.vars = c('NDD', 'NT_X', 'NDWS', 'NWLD', 'NWLD50', 'NWLD90','SHI', 'gSeason', 'SLGP', 'LGP'), 
              .funs = ~round(. , 0))
  
  data_cons <- bind_rows(past, future)  %>% 
    mutate(time1 = dplyr::case_when(time == 'Historic' ~ 1, 
                                    time == '2021-2040'~ 2, 
                                    time == '2041-2060'~ 3,   
                                    TRUE ~ NA_real_)) %>% 
    dplyr::select(time, time1 ,everything())
  
  # =---------------------------------------------------
  spi_past <- fst::fst( glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/past/{iso3}_spi_drought.fst') )%>% 
    tibble::as_tibble() %>% 
    mutate(time = 'Historic')
  
  spi_future <- tibble( file = list.files(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/{gcm}/'), full.names = TRUE, recursive = TRUE, pattern = '_spi_drought') ) %>%
    dplyr::filter(!grepl('_monthly', file) & !grepl('_old', file)) %>%
    mutate(shot_file = str_remove(file, pattern = glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/'))) %>% 
    mutate(data = purrr::map(.x = file, .f = function(x){x <- fst::fst(x) %>% tibble::as_tibble()})) %>%
    mutate(str_split(shot_file, '/') %>% 
             purrr::map(.f = function(x){tibble(gcm = x[1], time = x[3])}) %>% 
             bind_rows()) %>% dplyr::select(gcm, time, data) %>% 
    unnest() %>% dplyr::select(-gcm) %>% 
    group_by(time,id,x,y,season,year) %>%
    summarise_all(~mean(. , na.rm =  TRUE))
  
  
  spi_dat <- bind_rows(spi_past, spi_future)  %>%
    mutate(time1 = dplyr::case_when(time == 'Historic' ~ 1, 
                                    time == '2021-2040'~ 2, 
                                    time == '2041-2060'~ 3,   
                                    TRUE ~ NA_real_)) %>% 
    mutate(year = as.numeric(year)) %>% mutate(SPI = spi * 100) %>% 
    dplyr::select(time, time1 , id, year, SPI, season, time) 
  
  data_cons <- full_join(data_cons, spi_dat) %>% drop_na(time1)
  
  return(data_cons)}
# =----------------------------------------------
summary_index <- function(Zone, data_init = data_cons, Period){
  
  season <-  names(Period)
  st <- tail(Period[[1]], n=1); lt <- Period[[1]][1]
  length_season <- case_when(st > lt ~ st - (lt-1), 
                             st < lt ~ (13 - st) + lt)
  
  days <- sum(lubridate::days_in_month(1:12)[Period[[1]]])
  
  data <-  data_init %>% filter(season == names(Period)) 
  # =--------------
  path <- glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/results/maps/{Zone}_{season}')
  dir.create(path,recursive = TRUE)  
  # =--------------
  
  if(Zone == 'all'){
    zone <- regions_all # %>% sf::as_Spatial() 
    var_s <- to_do %>% mutate( Regions = 'all', Livehood_z = 'all', Short_Name = 'all') %>% 
      mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()}) %>% 
      group_by(ISO3, Country, Regions, Livehood_z, Short_Name ) %>% 
      summarise_all(. , sum, na.rm = TRUE) %>% ungroup()
    
    Name = 'General'
  }else{
    zone <- filter(regions_all, region == Zone) # %>% sf::as_Spatial() 
    var_s <- to_do %>% filter(Regions == Zone) %>%
      mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()})
    
    Name = filter(to_do, Regions  == Zone)$Short_Name   
  }
  
  # =-------------------------------
  vars <- dplyr::select(var_s, -ISO3, -Country, -Regions, -Livehood_z, -Short_Name) %>% 
    tidyr::gather(key = 'var',value = 'value')  %>% 
    filter(value > 0) %>% pull(var)
  
  if(sum(vars == 'NWLD') == 1){vars <- c(vars, "NWLD50", "NWLD90")}
  
  if(sum(vars == 'THI') == 1){
    vars <- c(vars[vars != 'THI'], glue::glue('THI_{0:3}'))}
  
  if(sum(vars == 'HSI') == 1){
    vars <- c(vars[vars != 'HSI'], glue::glue('HSI_{0:3}'))}
  # =-------------------------------
  
  
  # Cortar por el crd. 
  crd <- data_init  %>% filter(season == names(Period))  %>% filter(year == 2019) %>% mutate(ISO = iso3)
  pnt <- crd %>% dplyr::select(x,y) %>% drop_na() %>% sp::SpatialPoints(coords = .)
  crs(pnt) <- crs(regions_all)
  # Filter coordinates that are present in the county
  pnt <- sp::over(pnt, sf::as_Spatial(regions_all)) %>% data.frame  %>% complete.cases() %>% which()
  crd <- crd[pnt,]
  id_f <- unique(crd$id)
  
  
  data <- data %>% filter(id %in% id_f) %>% dplyr::select(time, id, vars) %>% 
    group_by(time, id) %>%summarise_all(~mean(. , na.rm =  TRUE)) %>%
    mutate_at(.vars = vars[vars %in% c('NDD', 'NT_X', 'NDWS', 'NWLD', 'NWLD50', 'NWLD90','SHI')], 
              .funs = ~round(. , 0)) %>%
    ungroup() %>%  unique() 
  
  
  # Lenght_season
  data <- mutate_at(data, .vars = vars[vars %in% c('NDD','NDWS',  'NWLD','NWLD50', 'NWLD90', 'NDD', 'NT_X')],
                    .funs = ~(./length_season) %>% round(. ,0) )
  
  # Transform variables. 
  if(sum(vars == 'SHI') == 1){
    data <- data %>% #dplyr::select(id, time, season, SHI) %>% 
      mutate(SHI = (SHI/days) )
  }
  
  if(sum(vars %in% c("SLGP", "LGP")) > 0 ){
    
    if(sum(vars == 'SLGP') > 0){vars <- c(vars, 'SLGP_CV')}
    vars_s <- vars[vars %in% c("SLGP", "LGP", 'SLGP_CV')]
    
    peta <- data %>% 
      filter(id %in% id_f) %>%
      dplyr::select(time, time1, id, year, gSeason, SLGP, LGP) 
    peta$date <- as.Date(peta$SLGP, origin = "2001-01-01") %>% lubridate::month()
    
    peta <- peta %>% 
      filter(date %in% Period[[1]]) %>% 
      group_by(time, time1, id, gSeason) %>%
      summarise_all(list(mean, sd), na.rm =  TRUE ) %>%
      ungroup() %>%
      mutate_at(.vars = c('SLGP_fn1', 'LGP_fn1'), .funs = ~round(. , 0)) %>%
      dplyr::select(-LGP_fn2) %>%
      rename(SLGP = 'SLGP_fn1', LGP = 'LGP_fn1', SLGP_CV = 'SLGP_fn2') %>%
      mutate(SLGP_CV = (SLGP_CV/SLGP) * 100) %>% 
      filter(gSeason == str_remove(season, 's') %>% as.numeric()) %>% 
      dplyr::select(id, time, time1, vars_s)
    
    data <- inner_join(data, peta)
  }
  
  # =---
  asa <- dplyr::select(data, -id) %>% 
    nest(-time) %>% 
    mutate(ind = purrr::map(.x = data, .f = function(pp){
      pp <- pp %>% drop_na() %>% 
        psych::describe() %>% 
        ungroup() }) )%>% 
    dplyr::select(-data) 
  
  
  Summary_final <- unnest(asa) %>% 
    mutate(Index = rep(rownames(asa$ind[[1]]), 3) ) %>% 
    dplyr::select(-n, -vars, -mad) %>%
    mutate(cod_name = Zone, 
           Livelihood_zone = Name, 
           Season = names(Period)) %>% 
    rename(Period = 'time') %>% 
    dplyr::select(Index, cod_name, Livelihood_zone, Period, Season, everything(.))
  
  # zone --- Name --- vars
  
  return(Summary_final)}
# =----------------------------------------------
read_monthly_data <- function(country, iso3){
  # Read all data complete. 
  past <- fst::fst( glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/past/{iso3}_indices_monthly.fst') )%>% 
    tibble::as_tibble() %>% 
    mutate(time = 'Historic')
  
  gcm <- c('INM-CM5-0', 'ACCESS-ESM1-5', 'EC-Earth3-Veg', 'MPI-ESM1-2-HR', 'MRI-ESM2-0')
  
  future <- tibble( file = list.files(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/{gcm}/'), full.names =  TRUE, recursive = TRUE, pattern = '_indices_monthly') ) %>%
    mutate(shot_file = str_remove(file, pattern = glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/'))) %>% 
    mutate(data = purrr::map(.x = file, .f = function(x){x <- fst::fst(x) %>% tibble::as_tibble()})) %>%
    mutate(str_split(shot_file, '/') %>% 
             purrr::map(.f = function(x){tibble(gcm = x[1], time = x[3])}) %>% 
             bind_rows()) %>% 
    dplyr::select(gcm, time, data) %>% 
    unnest()
  
  future  <- future %>% dplyr::select(-gcm) %>% 
    group_by(time,id,x,y,season,year) %>%
    summarise_all(~mean(. , na.rm =  TRUE)) %>%
    mutate_at(.vars = c('NDD', 'NT_X', 'NDWS', 'NWLD', 'NWLD50', 'NWLD90','SHI', 'gSeason', 'SLGP', 'LGP'), 
              .funs = ~round(. , 0))
  
  data_cons <- bind_rows(past, future)  %>% 
    mutate(time1 = dplyr::case_when(time == 'Historic' ~ 1, 
                                    time == '2021-2040'~ 2, 
                                    time == '2041-2060'~ 3,   
                                    TRUE ~ NA_real_)) %>% 
    dplyr::select(time, time1 ,everything())
  
  return(data_cons)}
# =----------------------------------------------
summary_monthly <- function(Zone, data_init = data_cons, Period){
  
  to_do <- dplyr::select(to_do, -SPI)
  season <-  names(Period)
  st <- tail(Period[[1]], n=1); lt <- Period[[1]][1]
  length_season <- case_when(st > lt ~ st - (lt-1), 
                             st < lt ~ (13 - st) + lt)
  
  days <- sum(lubridate::days_in_month(1:12)[Period[[1]]])
  
  data <-  data_init %>% filter(season == names(Period)) 
  # =--------------
  #path <- glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/results/maps/{Zone}_{season}')
  #dir.create(path,recursive = TRUE)  
  # =--------------
  
  if(Zone == 'all'){
    zone <- regions_all # %>% sf::as_Spatial() 
    var_s <- to_do %>% mutate( Regions = 'all', Livehood_z = 'all', Short_Name = 'all') %>% 
      mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()}) %>% 
      group_by(ISO3, Country, Regions, Livehood_z, Short_Name ) %>% 
      summarise_all(. , sum, na.rm = TRUE) %>% ungroup()
    
    Name = 'General'
  }else{
    zone <- filter(regions_all, region == Zone) # %>% sf::as_Spatial() 
    var_s <- to_do %>% filter(Regions == Zone) %>%
      mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()})
    
    Name = filter(to_do, Regions  == Zone)$Short_Name   
  }
  
  # =-------------------------------
  vars <- dplyr::select(var_s, -ISO3, -Country, -Regions, -Livehood_z, -Short_Name) %>% 
    tidyr::gather(key = 'var',value = 'value')  %>% 
    filter(value > 0) %>% pull(var)
  
  if(sum(vars == 'NWLD') == 1){vars <- c(vars, "NWLD50", "NWLD90")}
  
  if(sum(vars == 'THI') == 1){
    vars <- c(vars[vars != 'THI'], glue::glue('THI_{0:3}'))}
  
  if(sum(vars == 'HSI') == 1){
    vars <- c(vars[vars != 'HSI'], glue::glue('HSI_{0:3}'))}
  # =-------------------------------
  
  
  # Cortar por el crd. 
  crd <- data_init  %>% filter(season == names(Period))  %>% filter(year == 2019) %>% mutate(ISO = iso3)
  pnt <- crd %>% dplyr::select(x,y) %>% drop_na() %>% sp::SpatialPoints(coords = .)
  crs(pnt) <- crs(regions_all)
  # Filter coordinates that are present in the county
  pnt <- sp::over(pnt, sf::as_Spatial(regions_all)) %>% data.frame  %>% complete.cases() %>% which()
  crd <- crd[pnt,]
  id_f <- unique(crd$id)
  
  
  data <- data %>% filter(id %in% id_f) %>% dplyr::select(time, id, vars) %>% 
    group_by(time, id) %>%summarise_all(~mean(. , na.rm =  TRUE)) %>%
    mutate_at(.vars = vars[vars %in% c('NDD', 'NT_X', 'NDWS', 'NWLD', 'NWLD50', 'NWLD90','SHI')], 
              .funs = ~round(. , 0)) %>%
    ungroup() %>%  unique() 
  
  
  # Lenght_season
  data <- mutate_at(data, .vars = vars[vars %in% c('NDD','NDWS',  'NWLD','NWLD50', 'NWLD90', 'NDD', 'NT_X')],
                    .funs = ~(./1) %>% round(. ,0) )
  
  # Transform variables. 
  if(sum(vars == 'SHI') == 1){
    data <- data %>% #dplyr::select(id, time, season, SHI) %>% 
      mutate(SHI = (SHI/days) )
  }
  
  if(sum(vars %in% c("SLGP", "LGP")) > 0 ){
    
    if(sum(vars == 'SLGP') > 0){vars <- c(vars, 'SLGP_CV')}
    vars_s <- vars[vars %in% c("SLGP", "LGP", 'SLGP_CV')]
    
    peta <- data %>% 
      filter(id %in% id_f) %>%
      dplyr::select(time, time1, id, year, gSeason, SLGP, LGP) 
    peta$date <- as.Date(peta$SLGP, origin = "2001-01-01") %>% lubridate::month()
    
    peta <- peta %>% 
      filter(date %in% Period[[1]]) %>% 
      group_by(time, time1, id, gSeason) %>%
      summarise_all(list(mean, sd), na.rm =  TRUE ) %>%
      ungroup() %>%
      mutate_at(.vars = c('SLGP_fn1', 'LGP_fn1'), .funs = ~round(. , 0)) %>%
      dplyr::select(-LGP_fn2) %>%
      rename(SLGP = 'SLGP_fn1', LGP = 'LGP_fn1', SLGP_CV = 'SLGP_fn2') %>%
      mutate(SLGP_CV = (SLGP_CV/SLGP) * 100) %>% 
      filter(gSeason == str_remove(season, 's') %>% as.numeric()) %>% 
      dplyr::select(id, time, time1, vars_s)
    
    data <- inner_join(data, peta)
  }
  
  # =---
  asa <- dplyr::select(data, -id) %>% 
    nest(-time) %>% 
    mutate(ind = purrr::map(.x = data, .f = function(pp){
      pp <- pp %>% drop_na() %>% 
        psych::describe() %>% 
        ungroup() }) )%>% 
    dplyr::select(-data) 
  
  
  Summary_final <- unnest(asa) %>% 
    mutate(Index = rep(rownames(asa$ind[[1]]), 3) ) %>% 
    dplyr::select(-n, -vars, -mad) %>%
    mutate(cod_name = Zone, 
           Livelihood_zone = Name, 
           Season = names(Period)) %>% 
    rename(Period = 'time') %>% 
    dplyr::select(Index, cod_name, Livelihood_zone, Period, Season, everything(.))
  
  # zone --- Name --- vars
  
  return(Summary_final)}
# =---------------------------------------------------


# =-------------------
# Seasonal Run. 
data_cons <- read_data_seasons(country, iso3)

index <- tibble(Zone = c('all', regions_all$region) ) %>% 
  mutate(index = purrr::map(.x = Zone, .f = function(x){
    indx <- list()
    for(i in 1:length(seasons)){
      indx[[i]] <- summary_index(Zone = x, data_init = data_cons, Period = seasons[i])
    }
    
    indx <- bind_rows(indx)
  })) %>% 
  unnest() %>% 
  dplyr::select(-cod_name )

write_csv(x = index, file = glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/sesons_ind.csv') )

# =-------------------


# =-------------------
# Monthly run.
seasons_month <- list(s1 = 1, s2 = 2, s3 = 3,s4 = 4,s5 = 5,s6 = 6,s7 = 7,s8 = 8,s9 = 9,s10 = 10,s11 = 11,s12 = 12)

# Poner esto en paralelo
tictoc::tic()
monthly_data <- read_monthly_data(country, iso3)
tictoc::toc() # 23.24 Min. 

index_mod <- tibble(Zone = c('all', regions_all$region) ) %>% 
  mutate(index = purrr::map(.x = Zone, .f = function(x){
    indx <- list()
    for(i in 1:length(seasons_month)){
      indx[[i]] <- summary_monthly(Zone = x, data_init = monthly_data, Period = seasons_month[i])
    }
    
    indx <- bind_rows(indx)
  })) %>% 
  unnest() %>% 
  dplyr::select(-cod_name )

write_csv(x = index_mod, file = glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/monthly_ind.csv'))
