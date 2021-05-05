# -------------------------------------------------- #
# Summary index  
# A. Esquivel, C. Saavedra, H. Achicanoy 
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

Other_parameters <- function(country, iso3){
  # Reading the tables of future indices. 
  to_do <<- readxl::read_excel('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/regions_ind.xlsx') %>% 
    dplyr::filter(ISO3 == iso3) %>% 
    dplyr::rename('Livehood_z' = 'Livelihood zones', 'NT_X'= "NT-X")
  
  
  # Regions shp
  regions_all <- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_regions/",tolower(iso3),"_regions.shp"))
  regions_all <- regions_all %>%  sf::st_as_sf() %>% 
    dplyr::group_by(region) %>% dplyr::summarise() %>% 
    dplyr::mutate(Short_Name = to_do$Short_Name)
  regions_all <<- regions_all
}
# =---------------------------------------------


# Functions. 
read_data_seasons <- function(country, iso3) {
  
  # Read all data complete. 
  past <- fst::fst(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/past/{iso3}_indices.fst') )%>% 
    tibble::as_tibble() %>% dplyr::mutate(time = 'Historic')
  
  gcm <- c('INM-CM5-0', 'ACCESS-ESM1-5', 'EC-Earth3-Veg', 'MPI-ESM1-2-HR', 'MRI-ESM2-0')
  
  future <- tibble(file = list.files(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/{gcm}/'), full.names =  TRUE, recursive = TRUE, pattern = '_indices') ) %>%
    dplyr::filter(!grepl('_monthly', file) & !grepl('_old', file)) %>% 
    dplyr::mutate(shot_file = str_remove(file, pattern = glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/'))) %>% 
    dplyr::mutate(data = purrr::map(.x = file, .f = function(x){x <- fst::fst(x) %>% tibble::as_tibble()})) %>%
    dplyr::mutate(stringr::str_split(shot_file, '/') %>% 
             purrr::map(.f = function(x){tibble(gcm = x[1], time = x[3])}) %>% 
             dplyr::bind_rows()) %>% 
    dplyr::select(gcm, time, data) %>% 
    tidyr::unnest()
  
  future  <- future %>% dplyr::select(-gcm) %>% 
    dplyr::group_by(time,id,x,y,season,year) %>%
    dplyr::summarise_all(~mean(. , na.rm =  TRUE)) %>%
    dplyr::mutate_at(.vars = c('NDD', 'NT_X', 'NDWS', 'NWLD', 'NWLD50', 'NWLD90','SHI', 'gSeason', 'SLGP', 'LGP'), 
              .funs = ~round(. , 0))
  
  data_cons <- dplyr::bind_rows(past, future)  %>% 
    dplyr::mutate(time1 = dplyr::case_when(time == 'Historic' ~ 1, 
                                    time == '2021-2040'~ 2, 
                                    time == '2041-2060'~ 3,   
                                    TRUE ~ NA_real_)) %>% 
    dplyr::select(time, time1 ,everything())
  
  # =---------------------------------------------------
  spi_past <- fst::fst( glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/past/{iso3}_spi_drought.fst') )%>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(time = 'Historic')
  
  spi_future <- tibble( file = list.files(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/{gcm}/'), full.names = TRUE, recursive = TRUE, pattern = '_spi_drought') ) %>%
    dplyr::filter(!grepl('_monthly', file) & !grepl('_old', file)) %>%
    dplyr::mutate(shot_file = str_remove(file, pattern = glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/'))) %>% 
    dplyr::mutate(data = purrr::map(.x = file, .f = function(x){x <- fst::fst(x) %>% tibble::as_tibble()})) %>%
    dplyr::mutate(stringr::str_split(shot_file, '/') %>% 
             purrr::map(.f = function(x){tibble(gcm = x[1], time = x[3])}) %>% 
             dplyr::bind_rows()) %>% dplyr::select(gcm, time, data) %>% 
    tidyr::unnest() %>% dplyr::select(-gcm) %>% 
    dplyr::group_by(time,id,x,y,season,year) %>%
    dplyr::summarise_all(~mean(. , na.rm =  TRUE))
  
  
  spi_dat <- dplyr::bind_rows(spi_past, spi_future)  %>%
    dplyr::mutate(time1 = dplyr::case_when(time == 'Historic' ~ 1, 
                                    time == '2021-2040'~ 2, 
                                    time == '2041-2060'~ 3,   
                                    TRUE ~ NA_real_)) %>% 
    dplyr::mutate(year = as.numeric(year)) %>% dplyr::mutate(SPI = spi * 100) %>% 
    dplyr::select(time, time1 , id, year, SPI, season, time) 
  
  data_cons <- dplyr::full_join(data_cons, spi_dat) %>% tidyr::drop_na(time1)
  
  return(data_cons)}
# =----------------------------------------------
summary_index <- function(Zone, data_init = data_cons, Period){
  
  season <-  names(Period)
  st <- tail(Period[[1]], n=1); lt <- Period[[1]][1]
  length_season <- length(Period[[1]])
  
  days <- sum(lubridate::days_in_month(1:12)[Period[[1]]])
  
  data <-  data_init %>% dplyr::filter(season == names(Period)) 
  # =--------------
  path <- glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/results/maps/{Zone}_{season}')
  dir.create(path,recursive = TRUE)  
  # =--------------
  
  if(Zone == 'all'){
    zone <- regions_all # %>% sf::as_Spatial() 
    var_s <- to_do %>% dplyr::mutate( Regions = 'all', Livehood_z = 'all', Short_Name = 'all') %>% 
      dplyr::mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()}) %>% 
      dplyr::group_by(ISO3, Country, Regions, Livehood_z, Short_Name ) %>% 
      dplyr::summarise_all(. , sum, na.rm = TRUE) %>% dplyr::ungroup()
    
    Name = 'General'
  }else{
    zone <- dplyr::filter(regions_all, region == Zone) # %>% sf::as_Spatial() 
    var_s <- to_do %>% dplyr::filter(Regions == Zone) %>%
      dplyr::mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()})
    
    Name = dplyr::filter(to_do, Regions  == Zone)$Short_Name   
  }
  
  # =-------------------------------
  vars <- dplyr::select(var_s, -ISO3, -Country, -Regions, -Livehood_z, -Short_Name) %>% 
    tidyr::gather(key = 'var',value = 'value')  %>% 
    dplyr::filter(value > 0) %>% pull(var)
  
  if(sum(vars == 'NWLD') == 1){vars <- c(vars, "NWLD50", "NWLD90")}
  
  if(sum(vars == 'THI') == 1){
    vars <- c(vars[vars != 'THI'], glue::glue('THI_{0:3}'))}
  
  if(sum(vars == 'HSI') == 1){
    vars <- c(vars[vars != 'HSI'], glue::glue('HSI_{0:3}'))}
  # =-------------------------------
  
  
  # Cortar por el crd. 
  crd <- data_init  %>% dplyr::filter(season == names(Period))  %>% dplyr::filter(year == 2019) %>% dplyr::mutate(ISO =  iso)
  pnt <- crd %>% dplyr::select(x,y) %>% 
    tidyr::drop_na() %>% sp::SpatialPoints(coords = .)
  crs(pnt) <- crs(regions_all)
  # dplyr::filter coordinates that are present in the county
  pnt <- sp::over(pnt, sf::as_Spatial(regions_all)) %>% data.frame  %>% complete.cases() %>% which()
  crd <- crd[pnt,]
  id_f <- unique(crd$id)
  
  
  data <- data %>% dplyr::filter(id %in% id_f) %>% dplyr::select(time, id, vars) %>% 
    dplyr::group_by(time, id) %>%dplyr::summarise_all(~mean(. , na.rm =  TRUE)) %>%
    dplyr::mutate_at(.vars = vars[vars %in% c('NDD', 'NT_X', 'NDWS', 'NWLD', 'NWLD50', 'NWLD90','SHI')], 
              .funs = ~round(. , 0)) %>%
    dplyr::ungroup() %>%  unique() 
  
  
  # Lenght_season
  data <- dplyr::mutate_at(data, .vars = vars[vars %in% c('NDD','NDWS',  'NWLD','NWLD50', 'NWLD90', 'NDD', 'NT_X')],
                    .funs = ~(./length_season) %>% round(. ,0) )
  
  # Transform variables. 
  if(sum(vars == 'SHI') == 1){
    data <- data %>% #dplyr::select(id, time, season, SHI) %>% 
      dplyr::mutate(SHI = (SHI/days) )
  }
  
  if(sum(vars %in% c("SLGP", "LGP")) > 0 ){
    
    if(sum(vars == 'SLGP') > 0){vars <- c(vars, 'SLGP_CV')}
    vars_s <- vars[vars %in% c("SLGP", "LGP", 'SLGP_CV')]
    
    peta <- data %>% 
      dplyr::filter(id %in% id_f) %>%
      dplyr::select(time, time1, id, year, gSeason, SLGP, LGP) 
    peta$date <- as.Date(peta$SLGP, origin = "2001-01-01") %>% lubridate::month()
    
    peta <- peta %>% 
      dplyr::filter(date %in% Period[[1]]) %>% 
      dplyr::group_by(time, time1, id, gSeason) %>%
      dplyr::summarise_all(list(mean, sd), na.rm =  TRUE ) %>%
      dplyr::ungroup() %>%
      dplyr::mutate_at(.vars = c('SLGP_fn1', 'LGP_fn1'), .funs = ~round(. , 0)) %>%
      dplyr::select(-LGP_fn2) %>%
      dplyr::rename(SLGP = 'SLGP_fn1', LGP = 'LGP_fn1', SLGP_CV = 'SLGP_fn2') %>%
      dplyr::mutate(SLGP_CV = (SLGP_CV/SLGP) * 100) %>% 
      dplyr::filter(gSeason == str_remove(season, 's') %>% as.numeric()) %>% 
      dplyr::select(id, time, time1, vars_s)
    
    data <- dplyr::inner_join(data, peta)
  }
  
  # =---
  asa <- dplyr::select(data, -id) %>% 
    tidyr::nest(-time) %>% 
    dplyr::mutate(ind = purrr::map(.x = data, .f = function(pp){
      pp <- pp %>% tidyr::drop_na() %>% 
        psych::describe() %>% 
        dplyr::ungroup() }) )%>% 
    dplyr::select(-data) 
  
  
  Summary_final <- tidyr::unnest(asa) %>% 
    dplyr::mutate(Index = rep(rownames(asa$ind[[1]]), 3) ) %>% 
    dplyr::select(-n, -vars, -mad) %>%
    dplyr::mutate(cod_name = Zone, 
           Livelihood_zone = Name, 
           Season = names(Period)) %>% 
    dplyr::rename(Period = 'time') %>% 
    dplyr::select(Index, cod_name, Livelihood_zone, Period, Season, everything(.))
  
  # zone --- Name --- vars
  
  return(Summary_final)}
# =----------------------------------------------
read_monthly_data <- function(country, iso3){
  # Read all data complete. 
  past <- fst::fst( glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/past/{iso3}_indices_monthly.fst') )%>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(time = 'Historic')
  
  gcm <- c('INM-CM5-0', 'ACCESS-ESM1-5', 'EC-Earth3-Veg', 'MPI-ESM1-2-HR', 'MRI-ESM2-0')
  
  # Run the process in parallel for the 30% of the pixels
  ncores <- 5
  future::plan(cluster, workers = ncores, gc = TRUE)
  
  
  future <- tibble( file = list.files(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/{gcm}/'), full.names =  TRUE, recursive = TRUE, pattern = '_indices_monthly') ) %>%
    dplyr::mutate(shot_file = str_remove(file, pattern = glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/'))) %>% 
    dplyr::mutate(data = furrr::future_map(.x = file, .f = function(x){x <- fst::fst(x) %>% tibble::as_tibble()})) %>%
    dplyr::mutate(stringr::str_split(shot_file, '/') %>% 
             purrr::map(.f = function(x){tibble(gcm = x[1], time = x[3])}) %>% 
             dplyr::bind_rows()) %>% 
    dplyr::select(gcm, time, data) %>% 
    tidyr::unnest()
  
  future:::ClusterRegistry("stop")
  gc(reset = T)
  
  future  <- future %>% dplyr::select(-gcm) %>% 
    dplyr::group_by(time,id,x,y,season,year) %>%
    dplyr::summarise_all(~mean(. , na.rm =  TRUE)) %>%
    dplyr::mutate_at(.vars = c('NDD', 'NT_X', 'NDWS', 'NWLD', 'NWLD50', 'NWLD90','SHI', 'gSeason', 'SLGP', 'LGP'), 
              .funs = ~round(. , 0))
  
  data_cons <- dplyr::bind_rows(past, future)  %>% 
    dplyr::mutate(time1 = dplyr::case_when(time == 'Historic' ~ 1, 
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
  length_season <- length(Period[[1]])
  
  days <- sum(lubridate::days_in_month(1:12)[Period[[1]]])
  
  data <-  data_init %>% dplyr::filter(season == names(Period)) 
  # =--------------
  if(Zone == 'all'){
    zone <- regions_all # %>% sf::as_Spatial() 
    var_s <- to_do %>% dplyr::mutate( Regions = 'all', Livehood_z = 'all', Short_Name = 'all') %>% 
      dplyr::mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()}) %>% 
      dplyr::group_by(ISO3, Country, Regions, Livehood_z, Short_Name ) %>% 
      dplyr::summarise_all(. , sum, na.rm = TRUE) %>% dplyr::ungroup()
    
    Name = 'General'
  }else{
    zone <- dplyr::filter(regions_all, region == Zone) # %>% sf::as_Spatial() 
    var_s <- to_do %>% dplyr::filter(Regions == Zone) %>%
      dplyr::mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()})
    
    Name = dplyr::filter(to_do, Regions  == Zone)$Short_Name   
  }
  
  # =-------------------------------
  vars <- dplyr::select(var_s, -ISO3, -Country, -Regions, -Livehood_z, -Short_Name) %>% 
    tidyr::gather(key = 'var',value = 'value')  %>% 
    dplyr::filter(value > 0) %>% pull(var)
  
  if(sum(vars == 'NWLD') == 1){vars <- c(vars, "NWLD50", "NWLD90")}
  
  if(sum(vars == 'THI') == 1){
    vars <- c(vars[vars != 'THI'], glue::glue('THI_{0:3}'))}
  
  if(sum(vars == 'HSI') == 1){
    vars <- c(vars[vars != 'HSI'], glue::glue('HSI_{0:3}'))}
  # =-------------------------------
  
  
  # Cortar por el crd. 
  crd <- data_init  %>% dplyr::filter(season == names(Period))  %>% dplyr::filter(year == 2019) %>% dplyr::mutate(ISO = iso)
  pnt <- crd %>% dplyr::select(x,y) %>% tidyr::drop_na() %>% sp::SpatialPoints(coords = .)
  crs(pnt) <- crs(regions_all)
  # dplyr::filter coordinates that are present in the county
  pnt <- sp::over(pnt, sf::as_Spatial(regions_all)) %>% data.frame  %>% complete.cases() %>% which()
  crd <- crd[pnt,]
  id_f <- unique(crd$id)
  
  
  data <- data %>% dplyr::filter(id %in% id_f) %>% dplyr::select(time, id, vars) %>% 
    dplyr::group_by(time, id) %>%dplyr::summarise_all(~mean(. , na.rm =  TRUE)) %>%
    dplyr::mutate_at(.vars = vars[vars %in% c('NDD', 'NT_X', 'NDWS', 'NWLD', 'NWLD50', 'NWLD90','SHI')], 
                     .funs = ~round(. , 0)) %>%
    dplyr::ungroup() %>%  unique() 
  
  
  # Lenght_season
  data <- dplyr::mutate_at(data, .vars = vars[vars %in% c('NDD','NDWS',  'NWLD','NWLD50', 'NWLD90', 'NDD', 'NT_X')],
                           .funs = ~(./1) %>% round(. ,0) )
  
  # Transform variables. 
  if(sum(vars == 'SHI') == 1){
    data <- data %>% #dplyr::select(id, time, season, SHI) %>% 
      dplyr::mutate(SHI = (SHI/days) )
  }
  
  if(sum(vars %in% c("SLGP", "LGP")) > 0 ){
    
    if(sum(vars == 'SLGP') > 0){vars <- c(vars, 'SLGP_CV')}
    vars_s <- vars[vars %in% c("SLGP", "LGP", 'SLGP_CV')]
    
    peta <- data %>% 
      dplyr::filter(id %in% id_f) %>%
      dplyr::select(time, time1, id, year, gSeason, SLGP, LGP) 
    peta$date <- as.Date(peta$SLGP, origin = "2001-01-01") %>% lubridate::month()
    
    peta <- peta %>% 
      dplyr::filter(date %in% Period[[1]]) %>% 
      dplyr::group_by(time, time1, id, gSeason) %>%
      dplyr::summarise_all(list(mean, sd), na.rm =  TRUE ) %>%
      dplyr::ungroup() %>%
      dplyr::mutate_at(.vars = c('SLGP_fn1', 'LGP_fn1'), .funs = ~round(. , 0)) %>%
      dplyr::select(-LGP_fn2) %>%
      dplyr::rename(SLGP = 'SLGP_fn1', LGP = 'LGP_fn1', SLGP_CV = 'SLGP_fn2') %>%
      dplyr::mutate(SLGP_CV = (SLGP_CV/SLGP) * 100) %>% 
      dplyr::filter(gSeason == str_remove(season, 's') %>% as.numeric()) %>% 
      dplyr::select(id, time, time1, vars_s)
    
    data <- dplyr::inner_join(data, peta)
  }
  
  # =---
  asa <- dplyr::select(data, -id) %>% 
    tidyr::nest(-time) %>% 
    dplyr::mutate(ind = purrr::map(.x = data, .f = function(pp){
      pp <- pp %>% tidyr::drop_na() %>% 
        psych::describe() %>% 
        dplyr::ungroup() }) )%>% 
    dplyr::select(-data) 
  
  
  Summary_final <- tidyr::unnest(asa) %>% 
    dplyr::mutate(Index = rep(rownames(asa$ind[[1]]), 3) ) %>% 
    dplyr::select(-n, -vars, -mad) %>%
    dplyr::mutate(cod_name = Zone, 
                  Livelihood_zone = Name, 
                  Season = names(Period)) %>% 
    dplyr::rename(Period = 'time') %>% 
    dplyr::select(Index, cod_name, Livelihood_zone, Period, Season, everything(.))
  
  # zone --- Name --- vars
  
  return(Summary_final)}
# =---------------------------------------------------
