# WFP Climate Risk Project
# =----------------------
# Graphs.  
# A. Esquivel - H. Achicanoy - C.Saavedra 
# =----------------------

# Paths
# OSys <- Sys.info()[1]
# root <<- switch(OSys,
#                 'Linux'   = '/home/jovyan/work/cglabs',
#                 'Windows' = '//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr')

# =-------------------------------------
# Parameters 
# iso3 <- 'GNB'
# country <- 'Guinea-Bissau'
# seasons <- list(s1 = c(5,6,7,8,9,10,11))
# Zone  <- 'all'
# =-------------------------------------

map_graphs <- function(iso3, country, seasons, Zone = 'all'){
  
  # Reading the tables of future indices. 
  to_do <<- readxl::read_excel('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/regions_ind.xlsx') %>% 
    dplyr::filter(ISO3 == iso3) %>% 
    dplyr::rename('Livehood_z' = 'Livelihood zones', 'NT_X'= "NT-X")
  # =----------------------------------------------
  # Read all shp ... 
  # =----------------------------------------------
  
  # Read GDAM's shp at administrative level 1. 
  shp <<- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_gadm/",country,"_GADM1.shp"))
  shp_sf <-  shp %>%  sf::st_as_sf() %>% 
    dplyr::group_by(NAME_0) %>% dplyr::summarise()
  shp_sf <<- shp_sf
  
  # Regions shp
  regions_all <- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_regions/",tolower(iso3),"_regions.shp"))
  regions_all <- regions_all %>%  sf::st_as_sf() %>% 
    dplyr::group_by(region) %>% dplyr::summarise() %>% 
    dplyr::mutate(Short_Name = to_do$Short_Name)
  regions_all <<- regions_all
  
  
  # =--- World boundaries.
  map_world <- raster::shapefile(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/shps/all_country/all_countries.shp')) %>% 
    sf::st_as_sf()
  map_world <<- map_world
  
  ctn <- map_world$CONTINENT[which(map_world$ISO3 == iso3)]
  ctn <- map_world %>% dplyr::filter(CONTINENT == ctn)
  ctn <<- ctn
  # =--- 
  
  
  # =--- water sources. 
  glwd1 <- raster::shapefile('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/shps/GLWD/glwd_1.shp' ) 
  crs(glwd1) <- crs(shp)
  
  glwd2 <- raster::shapefile('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/shps/GLWD/glwd_2.shp' ) 
  crs(glwd2) <- crs(shp)
  
  if(!(iso3  %in% c('NPL', 'PAK')) ){
    ext.sp <- raster::crop(glwd1, raster::extent(shp))
    glwd1 <-  rgeos::gSimplify(ext.sp, tol = 0.05, topologyPreserve = TRUE) %>%
      sf::st_as_sf()
    
    ext.sp2 <- raster::crop(glwd2, raster::extent(shp))
    glwd2 <- rgeos::gSimplify(ext.sp2, tol = 0.05, topologyPreserve = TRUE) %>%
      sf::st_as_sf()
  }else{   
    glwd1 <-  rgeos::gSimplify(glwd1, tol = 0.05, topologyPreserve = TRUE) %>%
      sf::st_as_sf()
    
    glwd2 <- rgeos::gSimplify(glwd2, tol = 0.05, topologyPreserve = TRUE) %>%
      sf::st_as_sf()
  }
  
  glwd1 <<-  glwd1
  glwd2 <<- glwd2
  # =--- 
  
  # =--------------------------------------------
  
  # =----------------------------------------------
  # Read all data complete. 
  
  past <- tidyft::parse_fst( glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/past/{iso3}_indices.fst') )%>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(time = 'Historic')
  
  
  gcm <- c('INM-CM5-0', 'ACCESS-ESM1-5', 'EC-Earth3-Veg', 'MPI-ESM1-2-HR', 'MRI-ESM2-0')
  
  ncores <- 5
  plan(cluster, workers = ncores, gc = TRUE)
  
  future <- tibble( file = list.files(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/{gcm}/'), full.names =  TRUE, recursive = TRUE, pattern = '_indices') ) %>%
    dplyr::filter(!grepl('_monthly', file) & !grepl('_old', file) & !grepl('zone', file)) %>%
    dplyr::mutate(shot_file = str_remove(file, pattern = glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/'))) %>% 
    dplyr::mutate(data = furrr::future_map(.x = file, .f = function(x){x <- tidyft::parse_fst(x) %>% tibble::as_tibble()})) %>%
    dplyr::mutate(str_split(shot_file, '/') %>% 
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
  
  # =---------------------------------------------------
  
  
  spi_past <- tidyft::parse_fst( glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/past/{iso3}_spi_drought.fst') )%>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(time = 'Historic')
  
  
  spi_future <- tibble( file = list.files(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/{gcm}/'), full.names = TRUE, recursive = TRUE, pattern = '_spi_drought') ) %>%
    dplyr::filter(!grepl('_monthly', file) & !grepl('_old', file)) %>%
    dplyr::mutate(shot_file = str_remove(file, pattern = glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/'))) %>% 
    dplyr::mutate(data = purrr::map(.x = file, .f = function(x){x <- tidyft::parse_fst(x) %>% tibble::as_tibble()})) %>%
    dplyr::mutate(str_split(shot_file, '/') %>% 
                    purrr::map(.f = function(x){tibble(gcm = x[1], time = x[3])}) %>% 
                    dplyr::bind_rows()) %>% 
    dplyr::select(gcm, time, data) %>% 
    tidyr::unnest() %>% dplyr::select(-gcm) %>% 
    dplyr::group_by(time,id,x,y,season,year) %>%
    dplyr::summarise_all(~mean(. , na.rm =  TRUE))
  
  
  spi_dat <- dplyr::bind_rows(spi_past, spi_future)  %>%
    dplyr::mutate(time1 = dplyr::case_when(time == 'Historic' ~ 1, 
                                           time == '2021-2040'~ 2, 
                                           time == '2041-2060'~ 3,   
                                           TRUE ~ NA_real_)) %>% 
    dplyr::mutate(year = as.numeric(year)) %>% 
    dplyr::mutate(SPI = spi * 100) %>% 
    dplyr::select(time, time1 , id, year, SPI, season, time) 
  
  data_cons <- dplyr::full_join(data_cons, spi_dat) %>% tidyr::drop_na(time1)
  # =----------------------------------------------------
  # =-  Climate ---
  Climate <- tidyft::parse_fst(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/observed_data/{iso3}/year/climate_1981_mod.fst')) %>% as_tibble()
  
  # Para los graphs... es importante tener en cuenta que se 
  # debe guardar las coordenadas, ya que sino no grafica correctamente. 
  coord <- Climate %>% dplyr::select(id, x, y) %>% unique()
  rm(Climate)
  
  # =----------------------------------------------
  mapping_g <- function(R_zone, iso3, country, Period = seasons, data_cons = data_cons, coord = coord){
    # R_zone <- to_do$Regions[1]
    season <-  names(Period)
    st <- tail(Period[[1]], n=1); lt <- Period[[1]][1]
    # length_season <- case_when(st > lt ~ st - (lt-1), 
    # st < lt ~ (13 - lt) + st)
    length_season <- length(Period[[1]])
    
    days <- sum(lubridate::days_in_month(1:12)[Period[[1]]])
    pet <- data_cons
    
    data_cons <-  data_cons %>% dplyr::filter(season == names(Period)) 
    # =--------------
    path <- glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/results/maps/{R_zone}_{season}')
    dir.create(path,recursive = TRUE)  
    # =--------------
    
    if(R_zone == 'all'){
      zone <- regions_all # %>% sf::as_Spatial() 
      var_s <- to_do %>% dplyr::mutate( Regions = 'all', Livehood_z = 'all', Short_Name = 'all') %>% 
        dplyr::mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()}) %>% 
        dplyr::group_by(ISO3, Country, Regions, Livehood_z, Short_Name ) %>% 
        dplyr::summarise_all(. , sum, na.rm = TRUE) %>% dplyr::ungroup()
      
      title = 'Country'
    }else{
      zone <- dplyr::filter(regions_all, region == R_zone) # %>% sf::as_Spatial() 
      var_s <- to_do %>% dplyr::filter(Regions == R_zone) %>%
        dplyr::mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()})
      
      title = dplyr::filter(to_do, Regions  == R_zone)$Short_Name   
    }
    
    # Aqui hacer el bufer --- menor tama?o. 
    zone_bufer <- sf::st_buffer(zone, dist = 0.05)  %>% 
      sf::st_union(.) %>% sf::as_Spatial()
    
    
    # =--------------------------------------------
    # Limits 
    xlims <<- sf::st_bbox(zone_bufer)[c(1, 3)]
    ylims <<- sf::st_bbox(zone_bufer)[c(2, 4)]
    
    
    vars <- dplyr::select(var_s, -ISO3, -Country, -Regions, -Livehood_z, -Short_Name) %>% 
      tidyr::gather(key = 'var',value = 'value')  %>% 
      dplyr::filter(value > 0) %>% dplyr::pull(var)
    
    if(sum(vars == 'NWLD') == 1){vars <- c(vars, "NWLD50", "NWLD90")}
    
    if(sum(vars == 'THI') == 1){
      vars <- c(vars[vars != 'THI'], glue::glue('THI_{0:3}'))
      vars <- c(vars, 'THI_23')}
    
    if(sum(vars == 'HSI') == 1){
      vars <- c(vars[vars != 'HSI'], glue::glue('HSI_{0:3}'))
      vars <- c(vars, 'HSI_23')}
    
    # Temporal
    basic_vars <- vars[!(vars %in% c("SLGP", "LGP" ))]
    
    # =-----------------
    
    # Cortar por el crd. 
    crd <- data_cons %>%
      dplyr::filter(year == 2019) %>% dplyr::mutate(ISO = iso3)
    pnt <- crd %>% dplyr::select(x,y) %>% tidyr::drop_na() %>% sp::SpatialPoints(coords = .)
    crs(pnt) <- crs(shp)
    # dplyr::filter coordinates that are present in the county
    pnt <- sp::over(pnt, zone_bufer) %>% data.frame  %>% complete.cases() %>% which()
    crd <- crd[pnt,]
    id_f <- unique(crd$id)
    
    coord_zone <- coord %>% dplyr::filter(id %in% id_f )
    
    # =----------------------------------------------------
    # Basic vars. 
    to_graph <- data_cons %>% dplyr::filter(id %in% id_f) %>%
      dplyr::mutate(THI_23 = THI_2 + THI_3, HSI_23 = HSI_2 + HSI_3) %>%
      dplyr::select(time, time1, id, basic_vars) %>%
      dplyr::group_by(time, time1, id) %>% 
      dplyr::summarise_all(~mean(. , na.rm =  TRUE)) %>%
      dplyr::mutate_at(.vars = basic_vars[basic_vars %in% c('NDD', 'NT_X', 'NDWS', 'NWLD', 'NWLD50', 'NWLD90','SHI')], 
                       .funs = ~round(. , 0)) %>%
      dplyr::ungroup() %>%  base::unique() %>% 
      dplyr::full_join(coord_zone, . )
    
    
    # Lenght_season
    to_graph <- dplyr::mutate_at(to_graph, .vars = basic_vars[basic_vars %in% c('NDD','NDWS',  'NWLD','NWLD50', 'NWLD90', 'NDD', 'NT_X')],
                                 .funs = ~(./length_season) %>% round(. ,0) )
    
    
    # Transform variables. 
    if(sum(basic_vars == 'SHI') == 1){
      to_graph <- to_graph %>% #dplyr::select(id, time, season, SHI) %>% 
        dplyr::mutate(SHI = (SHI/days) )
    }
    
    if(sum(vars %in% c("SLGP", "LGP")) > 0 ){
      
      if(sum(vars == 'SLGP') > 0){vars <- c(vars, 'SLGP_CV')}
      vars_s <- vars[vars %in% c("SLGP", "LGP", 'SLGP_CV')]
      
      peta <- data_cons %>% 
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
      
      to_graph <- dplyr::inner_join(to_graph, peta)
    }
    
    # =----------------------------------------------------
    # MAX section
    if(sum(vars == 'NWLD') > 0){
      max_add <- data_cons %>% dplyr::filter(id %in% id_f) %>%
        dplyr::select(time, time1, id,  'NWLD', 'NWLD50', 'NWLD90') %>%
        dplyr::group_by(time, time1, id) %>% tidyr::drop_na() %>% 
        dplyr::summarise_all(~max(. , na.rm =  TRUE)) %>%
        dplyr::mutate_at(.vars =  c('NWLD', 'NWLD50', 'NWLD90'), 
                         .funs = ~round(. , 0)) %>%
        dplyr::mutate_at(.vars =  c('NWLD', 'NWLD50', 'NWLD90'), 
                         .funs = ~(./length_season) %>% round(. ,0) ) %>% # Here
        dplyr::ungroup() %>%  unique() %>% 
        setNames(c('time','time1','id','NWLD_max', 'NWLD50_max', 'NWLD90_max'))
      
      vars <- c(vars, 'NWLD_max', 'NWLD50_max', 'NWLD90_max')
      
      
      to_graph <- to_graph %>% dplyr::inner_join(. , max_add)
    }
    
    # =----------------------------------------------------
    special_base <- dplyr::select(pet, id, time, time1, season, ATR, AMT) %>%
      dplyr::filter(id %in% id_f) %>%
      dplyr::group_by(time, time1, season, id) %>% 
      dplyr::summarise_all(.funs = mean, na.rm = TRUE) %>% 
      dplyr::ungroup()
    
    Special_limits <- special_base %>% 
      dplyr::select(ATR, AMT) %>% 
      dplyr::summarise_all(.funs = c('min', 'max'), na.rm = TRUE)
    # =----------------------------------------------------
    
    
    max_lt <- to_graph %>% dplyr::select(contains('_max')) %>% 
      dplyr::summarise_all(.funs = c('min', 'max'), na.rm = TRUE) 
    
    limits <- to_graph %>% dplyr::select(-id, -x, -y, -time1 , -ATR, -AMT, -contains('_max')) %>% 
      dplyr::summarise_all(.funs = c('min', 'max'), na.rm = TRUE) %>% 
      bind_cols(max_lt) %>% 
      dplyr::bind_cols(Special_limits, . )
    
    var_toG <- names(dplyr::select(to_graph, -id, -x, -y, -time, -time1))
    
    for(i in 1:length(var_toG)){
      j <- c('_min' , '_max') 
      
      my_limits <- dplyr::select(limits, glue::glue('{var_toG[i]}{j}' )) %>% setNames(c('min', 'max')) %>% as.numeric()
      my_breaks <- round(seq(my_limits[1], my_limits[2],  length.out= 4), 0)
      my_limits <- c(ifelse(my_limits[1] > my_breaks[1], my_breaks[1], my_limits[1]) ,ifelse(my_limits[2] < my_breaks[4], my_breaks[4], my_limits[2]))
      
      if(var_toG[i] %in% c('SHI', glue::glue('THI_{0:3}'), glue::glue('HSI_{0:3}'), 'THI_23', 'HSI_23')){
        my_limits <- c(0, 1)
        my_breaks <- c(0, 0.3, 0.6, 1)
      }
      
      if(var_toG[i] %in% c('NDD','NDWS', 'NWLD','NWLD50', 'NWLD90', 'NDD', 'NT_X', 'NWLD_max','NWLD50_max', 'NWLD90_max')){
        my_limits <- c(0, 31)
        my_breaks <- c(0, 10, 20, 31)
      } 
      
      pattern <- case_when(
        var_toG[i] == "ATR" ~ 'ATR\n(mm/season)',
        var_toG[i] == "AMT" ~ 'AMT\n(\u00B0C)' ,
        var_toG[i] == "TAI" ~ 'TAI\nAridity',
        var_toG[i] == "SLGP" ~ 'SLGP\n(Day of\nthe year)', 
        var_toG[i] == "LGP" ~ 'LGP\n(days)', 
        var_toG[i] == 'NDD' ~ 'NDD\n(days/month)', 
        var_toG[i] == 'NDWS' ~ 'NDWS\n(days/month)', 
        var_toG[i] == 'NT_X' ~ 'NT_X\n(days/month)', 
        var_toG[i] == 'NWLD' ~ 'NWLD\n(days/month)', 
        var_toG[i] == 'NWLD50' ~ 'NWLD50\n(days/month)', 
        var_toG[i] == 'NWLD90' ~ 'NWLD90\n(days/month)', 
        var_toG[i] == 'P5D' ~ 'P5D\n(mm/5 days)', 
        var_toG[i] == 'P95' ~ 'P95\n(mm/day)',
        var_toG[i] == 'IRR' ~ 'IRR', 
        var_toG[i] == 'SHI' ~ 'SHI\n(prob)',
        var_toG[i] == 'HSI_0' ~ 'HSI_0\n(prob)', 
        var_toG[i] == 'HSI_1' ~ 'HSI_1\n(prob)',
        var_toG[i] == 'HSI_2' ~ 'HSI_2\n(prob)', 
        var_toG[i] == 'HSI_3' ~ 'HSI_3\n(prob)',
        var_toG[i] == 'HSI_23' ~ 'HSI_23\n(prob)',
        var_toG[i] == 'THI_0' ~ 'THI_0\n(prob)', 
        var_toG[i] == 'THI_1' ~ 'THI_1\n(prob)',
        var_toG[i] == 'THI_2' ~ 'THI_2\n(prob)', 
        var_toG[i] == 'THI_3' ~ 'THI_3\n(prob)',
        var_toG[i] == 'THI_23' ~ 'THI_23\n(prob)',
        var_toG[i] == 'SPI' ~ 'SPI\n(% area)',
        var_toG[i] == 'NWLD_max' ~ 'NWLD_max\n(days/month)', 
        var_toG[i] == 'NWLD50_max' ~ 'NWLD50_max\n(days/month)', 
        var_toG[i] == 'NWLD90_max' ~ 'NWLD90_max\n(days/month)',
        TRUE ~ var_toG[i])
      
      
      if(var_toG[i] == 'ATR'){
        
        mop <- scale_fill_gradientn(limits =  my_limits, 
                                    breaks = unique(my_breaks),
                                    labels = unique(my_breaks),
                                    colours = blues9, 
                                    guide = guide_colourbar(barwidth = 25, 
                                                            label.theme = element_text(angle = 25, size = 35)))
      }else if(var_toG[i] == 'AMT'){
        
        mop <- scale_fill_gradient(limits =  my_limits, 
                                   breaks = unique(my_breaks),
                                   labels = unique(my_breaks),
                                   low = "yellow", high = "red",
                                   guide = guide_colourbar(barwidth = 25,  
                                                           label.theme = element_text(angle = 25, size = 35)))
      }else{
        
        mop <- scale_fill_viridis_c(limits = my_limits, 
                                    breaks = unique(my_breaks),
                                    labels = unique(my_breaks),
                                    guide = guide_colourbar(barwidth = 20, 
                                                            label.theme = element_text(angle = 25, size = 35))) 
      }    
      
      ggplot() +
        geom_tile(data = tidyr::drop_na(to_graph, !!rlang::sym(var_toG[i]) ), aes(x = x, y = y, fill = !!rlang::sym(var_toG[i])  )) +
        geom_sf(data = glwd1, fill = 'lightblue', color = 'lightblue') +
        geom_sf(data = glwd2, fill = 'lightblue', color = 'lightblue') +
        geom_sf(data = ctn, fill = NA, color = gray(.5)) +
        # geom_sf(data = shp_sf, fill = NA, color = gray(.1)) +
        geom_sf(data = zone, fill = NA, color = 'black') +
        mop +
        labs(fill = pattern, x = NULL, y = NULL, title = title) +
        coord_sf(xlim = xlims, ylim = ylims) +
        facet_grid(~time1, labeller = as_labeller( c('1' = "Historic", '2' = "2021-2040", '3' = '2041-2060'))) +
        theme_bw() + 
        theme(legend.position = 'bottom', text = element_text(size=35), 
              strip.background = element_rect(colour = "black", fill = "white"),
              axis.text.x=element_blank(), axis.text.y=element_blank(),
              # axis.ticks.x=element_blank(), axis.ticks.y=element_blank(), 
              legend.title=element_text(size=35), 
              legend.spacing = unit(5, units = 'cm'),
              legend.spacing.x = unit(1.0, 'cm'), plot.title = element_text(hjust = 0.5)) 
      
      ggsave(glue::glue('{path}/C_{var_toG[i]}.png') , width = 15, height = 10, dpi = 300)
      
    }
    
    # =-------------------------------------
    # Anomaly Maps 
    if(length(unique(to_graph$time)) == 3){
      F2030 <- to_graph %>% dplyr::filter(time == '2021-2040') %>% 
        dplyr::select(-x, -y, -time,-time1)
      F2050 <- to_graph %>% dplyr::filter(time == '2041-2060') %>%
        dplyr::select(-x, -y, -time,-time1) 
      HT <- to_graph %>% dplyr::filter(time == 'Historic') %>% 
        dplyr::select(-x, -y, -time,-time1) 
      
      # =---
      px1 <- dplyr::intersect(unique(HT$id), unique(F2030$id))
      px2 <- dplyr::intersect(unique(HT$id), unique(F2050$id))
      px <- dplyr::intersect(px1, px2)
      
      HT <- dplyr::filter(HT, id %in% px)
      F2030 <- dplyr::filter(F2030, id %in% px)
      F2050 <- dplyr::filter(F2050, id %in% px)
      
      rm(px1, px2)
      # =---
      
      for(i in 2:ncol(HT)){F2030[,i] <- F2030[,i] - HT[,i]
      F2050[,i] <- F2050[,i] - HT[,i]}
      
      F2030$ATR <- (F2030$ATR/HT$ATR)*100
      F2050$ATR <- (F2050$ATR/HT$ATR)*100
      
      anomalies <- dplyr::bind_rows(dplyr::mutate(F2030, time1 = '2021-2040'), dplyr::mutate(F2050, time1 = '2041-2060')) %>%
        dplyr::inner_join(  dplyr::select(dplyr::filter(to_graph, time1 == 2), id, x, y), .)
      
      # =---------
      F2030_1 <- special_base %>% dplyr::filter(time == '2021-2040') %>% 
        dplyr::select(-time,-time1)
      F2050_1 <- special_base %>% dplyr::filter(time == '2041-2060') %>%
        dplyr::select(-time,-time1) 
      HT_1 <- special_base %>% dplyr::filter(time == 'Historic') %>% 
        dplyr::select(-time,-time1) 
      
      # =---
      px1 <- dplyr::intersect(unique(HT_1$id), unique(F2030_1$id))
      px2 <- dplyr::intersect(unique(HT_1$id), unique(F2050_1$id))
      px <- dplyr::intersect(px1, px2)
      
      HT_1 <- dplyr::filter(HT_1, id %in% px)
      F2030_1 <- dplyr::filter(F2030_1, id %in% px)
      F2050_1 <- dplyr::filter(F2050_1, id %in% px)
      
      rm(px1, px2)
      # =---
      
      for(i in 2:ncol(HT_1)){F2030_1[,i] <- F2030_1[,i] - HT_1[,i]
      F2050_1[,i] <- F2050_1[,i] - HT_1[,i]}
      
      F2030_1$ATR <- (F2030_1$ATR/HT_1$ATR)*100
      F2050_1$ATR <- (F2050_1$ATR/HT_1$ATR)*100
      
      sp_anom_l <-dplyr::bind_rows(dplyr::mutate(F2030_1, time1 = '2021-2040'), dplyr::mutate(F2050_1, time1 = '2041-2060')) %>% dplyr::select(ATR, AMT) %>% 
        dplyr::summarise_all(.funs = c('min', 'max'), na.rm = TRUE)
      # =---------
      
      max_lt <- anomalies %>% dplyr::select(contains('_max')) %>% 
        dplyr::summarise_all(.funs = c('min', 'max'), na.rm = TRUE) 
      
      limits <- anomalies %>% dplyr::select(-id, -x, -y, -time1 , -ATR, -AMT, -contains('_max')) %>% 
        dplyr::summarise_all(.funs = c('min', 'max'), na.rm = TRUE)  %>%
        bind_cols(max_lt) %>% 
        bind_cols(sp_anom_l , . )
      
      
      for(i in 1:length(var_toG)){
        j <- c('_min' , '_max')
        my_limits <- dplyr::select(limits, glue::glue('{var_toG[i]}{j}' )) %>% setNames(c('min', 'max')) %>% as.numeric()
        my_breaks <- round(seq(my_limits[1], my_limits[2],  length.out= 4), 0)
        my_limits <- c(ifelse(my_limits[1] > my_breaks[1], my_breaks[1], my_limits[1]) ,ifelse(my_limits[2] < my_breaks[4], my_breaks[4], my_limits[2]))
        
        
        if(var_toG[i] %in% c('SHI', glue::glue('THI_{0:3}'), glue::glue('HSI_{0:3}'), 'THI_23', 'HSI_23')){
          my_limits <- c(-1, 1)
          my_breaks <- c(-1, -0.5, 0, 0.5, 1)
        }
        
        
        pattern <- case_when(
          var_toG[i] == "ATR" ~ 'ATR\n(%)',
          var_toG[i] == "AMT" ~ 'AMT\n(\u00B0C)' ,
          var_toG[i] == "TAI" ~ 'TAI\nAridity',
          var_toG[i] == "SLGP" ~ 'SLGP\n(Day of\nthe year)', 
          var_toG[i] == "LGP" ~ 'LGP\n(days)', 
          var_toG[i] == 'NDD' ~ 'NDD\n(days/month)', 
          var_toG[i] == 'NDWS' ~ 'NDWS\n(days/month)', 
          var_toG[i] == 'NT_X' ~ 'NT_X\n(days/month)', 
          var_toG[i] == 'NWLD' ~ 'NWLD\n(days/month)', 
          var_toG[i] == 'NWLD50' ~ 'NWLD50\n(days/month)', 
          var_toG[i] == 'NWLD90' ~ 'NWLD90\n(days/month)', 
          var_toG[i] == 'P5D' ~ 'P5D\n(mm/5 days)', 
          var_toG[i] == 'P95' ~ 'P95\n(mm/day)',
          var_toG[i] == 'IRR' ~ 'IRR', 
          var_toG[i] == 'SHI' ~ 'SHI\n(prob)',
          var_toG[i] == 'HSI_0' ~ 'HSI_0\n(prob)', 
          var_toG[i] == 'HSI_1' ~ 'HSI_1\n(prob)',
          var_toG[i] == 'HSI_2' ~ 'HSI_2\n(prob)', 
          var_toG[i] == 'HSI_3' ~ 'HSI_3\n(prob)',
          var_toG[i] == 'HSI_23' ~ 'HSI_23\n(prob)',
          var_toG[i] == 'THI_0' ~ 'THI_0\n(prob)', 
          var_toG[i] == 'THI_1' ~ 'THI_1\n(prob)',
          var_toG[i] == 'THI_2' ~ 'THI_2\n(prob)', 
          var_toG[i] == 'THI_3' ~ 'THI_3\n(prob)',
          var_toG[i] == 'THI_23' ~ 'THI_23\n(prob)',
          var_toG[i] == 'SPI' ~ 'SPI\n(% area)',
          var_toG[i] == 'NWLD_max' ~ 'NWLD_max\n(days/month)', 
          var_toG[i] == 'NWLD50_max' ~ 'NWLD50_max\n(days/month)', 
          var_toG[i] == 'NWLD90_max' ~ 'NWLD90_max\n(days/month)',
          TRUE ~ var_toG[i])
        
        
        mop <- scale_fill_gradient2(#low = '#A50026', mid = 'white', high = '#000099',
          low = '#A50026', mid = 'white', high = '#000099',
          limits = my_limits,  
          breaks = unique(my_breaks),
          labels = unique(my_breaks),
          guide = guide_colourbar(barwidth = 20, label.theme = element_text(angle = 25, size = 35))) 
        
        if(var_toG[i] == 'AMT'){
          mop <- scale_fill_gradient2(
            # Tecnicamente esta bien, pero revisar esta parte. 
            # Los positivos estan con colores azules en vez de rojos. 
            low = '#000099', mid = 'white', high = '#A50026',
            limits = my_limits,  
            breaks = unique(my_breaks),
            labels = unique(my_breaks),
            guide = guide_colourbar(barwidth = 20, label.theme = element_text(angle = 25, size = 35))) 
        }
        
        ggplot() +
          geom_tile(data = tidyr::drop_na(anomalies, !!rlang::sym(var_toG[i]) ), aes(x = x, y = y, fill = !!rlang::sym(var_toG[i])  )) +
          geom_sf(data = glwd1, fill = 'lightblue', color = 'lightblue') +
          geom_sf(data = glwd2, fill = 'lightblue', color = 'lightblue') +
          geom_sf(data = ctn, fill = NA, color = gray(.5)) +
          # geom_sf(data = shp_sf, fill = NA, color = gray(.1)) +
          geom_sf(data = zone, fill = NA, color = 'black') +
          mop + 
          labs(fill = pattern, x = NULL, y = NULL, title = title) +
          scale_y_continuous(breaks = round(ylims, 2), n.breaks = 3) +
          scale_x_continuous(breaks = round(xlims, 2), n.breaks = 3) +
          coord_sf(xlim = xlims, ylim = ylims) +
          facet_grid(~time1) +
          theme_bw() + 
          theme(legend.position = 'bottom', text = element_text(size=35), 
                axis.text.x=element_blank(), axis.text.y=element_blank(),
                # axis.ticks.x=element_blank(), axis.ticks.y=element_blank(), 
                strip.background = element_rect(colour = "black", fill = "white"),
                legend.title=element_text(size=35), 
                legend.spacing = unit(5, units = 'cm'),
                legend.spacing.x = unit(1.0, 'cm'), plot.title = element_text(hjust = 0.5)) 
        
        ggsave(glue::glue('{path}/Anom_{var_toG[i]}.png') , width = 15, height = 10, dpi = 300)
      }
      
    }
    # =-------------------------------------
    
    ###### Categorization... 
    labels <- function(x){
      Q_clas <- quantile(x, c(0.25, 0.5, 0.75), na.rm =TRUE)
      
      return(Q_clas)}
    
    transform_q <- function(x){
      Q_clas <- quantile(x, c(0.25, 0.5, 0.75), na.rm =TRUE)
      
      x <- case_when(x <= Q_clas[1] ~ 1, 
                     x > Q_clas[1] & x <= Q_clas[2]~ 2, 
                     x > Q_clas[2] & x <= Q_clas[3]~ 3, 
                     x > Q_clas[3] ~ 4, TRUE ~ x) 
      return(x)}
    
    # names(to_do)
    # "SPI"  "NDWS"  "NWLD" "NT_X"    
    class_1 <- to_graph %>%
      dplyr::mutate_at(.vars = vars(var_toG), .funs = transform_q) 
    
    # =---------------
    ATR_class <- function(x){
      Q_clas <- quantile(special_base$ATR, c(0.25, 0.5, 0.75), na.rm =TRUE)
      x <- case_when(x <= Q_clas[1] ~ 1, 
                     x > Q_clas[1] & x <= Q_clas[2]~ 2, 
                     x > Q_clas[2] & x <= Q_clas[3]~ 3, 
                     x > Q_clas[3] ~ 4, TRUE ~ x) 
      return(x)}
    
    AMT_class <- function(x){
      Q_clas <- quantile(special_base$AMT, c(0.25, 0.5, 0.75), na.rm =TRUE)
      x <- case_when(x <= Q_clas[1] ~ 1, 
                     x > Q_clas[1] & x <= Q_clas[2]~ 2, 
                     x > Q_clas[2] & x <= Q_clas[3]~ 3, 
                     x > Q_clas[3] ~ 4, TRUE ~ x) 
      return(x)}
    
    class_1 <- to_graph %>% dplyr::select(id, time, time1, ATR, AMT) %>%
      dplyr::mutate(ATR = ATR_class(ATR),
                    AMT = AMT_class(AMT)) %>% 
      dplyr::left_join( . ,  dplyr::select(class_1,  -AMT, -ATR)) %>% 
      dplyr::select(id,x,y,time,time1, everything(.))
    # =---------------
    
    
    
    # =--------------------------------------------------
    if(sum(var_toG %in% c(glue::glue('THI_{0:3}'), glue::glue('HSI_{0:3}'), 'SHI', 'HSI_23', 'THI_23') ) > 0 ){
      var_Q <- var_toG[-which(var_toG %in% c(glue::glue('THI_{0:3}'), glue::glue('HSI_{0:3}'), 'SHI', 'HSI_23', 'THI_23'))]
    }else{var_Q <- var_toG}
    
    
    # Quantile maps. 
    for(i in 1:length(var_Q)){
      
      Q_q <- round(labels(dplyr::pull(to_graph[, var_Q[i]])), 0)
      
      
      if(var_Q[i] == 'ATR'){
        Q_q <-  round(quantile(special_base$ATR, c(0.25, 0.5, 0.75), na.rm =TRUE), 0)
      }
      
      if(var_Q[i] == 'AMT'){
        Q_q <-  round(Q_clas <- quantile(special_base$AMT, c(0.25, 0.5, 0.75), na.rm =TRUE), 0)
      }
      
      labs_qq <- c('1' = glue::glue('{var_Q[i]} <= {Q_q[1]}'),
                   '2' = glue::glue('{Q_q[1]} < {var_Q[i]} <= {Q_q[2]}'), 
                   '3' = glue::glue('{Q_q[2]} < {var_Q[i]} <= {Q_q[3]}'), 
                   '4' = glue::glue('{var_Q[i]} > {Q_q[3]}'))
      
      pattern <- case_when(
        var_Q[i] == "ATR" ~ '(mm/season)',
        var_Q[i] == "AMT" ~ '(\u00B0C)' ,
        var_Q[i] == "TAI" ~ 'Aridity',
        var_Q[i] == "SLGP" ~ '(Day of\nthe year)', 
        var_Q[i] == "LGP" ~ '(days)', 
        var_Q[i] == 'NDD' ~ '(days/month)', 
        var_Q[i] == 'NDWS' ~ '(days/month)', 
        var_Q[i] == 'NT_X' ~ '(days/month)', 
        var_Q[i] == 'NWLD' ~ '(days/month)', 
        var_Q[i] == 'NWLD50' ~ '(days/month)', 
        var_Q[i] == 'NWLD90' ~ '(days/month)', 
        var_Q[i] == 'P5D' ~ '(mm/5 days)', 
        var_Q[i] == 'P95' ~ '(mm/day)',
        var_Q[i] == 'IRR' ~ 'IRR', 
        var_toG[i] == 'SPI' ~ '(% area)',
        var_toG[i] == 'NWLD_max' ~ 'NWLD_max\n(days/month)', 
        var_toG[i] == 'NWLD50_max' ~ 'NWLD50_max\n(days/month)', 
        var_toG[i] == 'NWLD90_max' ~ 'NWLD90_max\n(days/month)',
        TRUE ~ var_Q[i])
      
      
      ggplot() +
        geom_tile(data =  tidyr::drop_na(class_1, !!rlang::sym(var_Q[i]) ), aes(x = x, y = y, fill = !!rlang::sym(var_Q[i]) %>% as.factor(.) )) +
        geom_sf(data = glwd1, fill = 'lightblue', color = 'lightblue') +
        geom_sf(data = glwd2, fill = 'lightblue', color = 'lightblue') +
        geom_sf(data = ctn, fill = NA, color = gray(.5)) +
        # geom_sf(data = shp_sf, fill = NA, color = gray(.1)) +
        geom_sf(data = zone, fill = NA, color = 'black') +
        scale_fill_manual(values = c( '1' = "#A7D96A", '2' = "#FFFFC1", '3' = "#FDAE61", '4' = '#D7191B', '5' = "#533104"), 
                          breaks = c('1', '2', '3', '4'), labels = labs_qq) +
        labs(fill = pattern, x = NULL, y = NULL, title = title) +
        scale_y_continuous(breaks = round(ylims, 2), n.breaks = 3) +
        scale_x_continuous(breaks = round(xlims, 2), n.breaks = 3) +
        coord_sf(xlim = xlims, ylim = ylims) +
        facet_grid(~time1, labeller = as_labeller( c('1' = "Historic", '2' = "2021-2040", '3' = '2041-2060'))) +
        theme_bw() + 
        guides(fill=guide_legend(nrow=2,byrow=TRUE)) +
        theme(legend.position = 'bottom', text = element_text(size=35),
              axis.text.x=element_blank(), axis.text.y=element_blank(),
              strip.background = element_rect(colour = "black", fill = "white"),
              legend.title=element_text(size=35),
              legend.spacing = unit(5, units = 'cm'),
              legend.spacing.x = unit(1.0, 'cm'), plot.title = element_text(hjust = 0.5))
      
      ggsave(glue::glue('{path}/Q_{var_Q[i]}.png') , width = 15, height = 10, dpi = 300)
    }
    
    
    # =------------------------------------------
    # Only graphs with special cat.
    # 1. TAI
    if(sum(names(to_graph) == 'TAI') > 0){
      b <- to_graph %>%  
        dplyr::select(id, time1,TAI) %>% 
        dplyr::mutate(TAI =case_when(TAI <= 40 ~ 1, 
                                     TAI > 40 & TAI <= 60~ 2, 
                                     TAI > 60 & TAI <= 80~ 3, 
                                     TAI > 80 ~ 4, 
                                     TRUE ~ TAI) )
      
      class_1 <- dplyr::full_join(class_1 %>% dplyr::select(-TAI), b)
    }
    
    # 2. 'NDWS', 'NDD' 
    if(sum(names(to_graph) %in% c('NDWS', 'NDD')) > 0){
      var_b <- names(to_graph)[names(to_graph) %in% c('NDWS', 'NDD')]
      b <- to_graph %>%
        dplyr::select(id, time1, var_b) %>% 
        dplyr::mutate_at(.vars = var_b, .funs = function(x){
          case_when(x <= 15 ~ 1, 
                    x > 15 & x <= 20 ~ 2,
                    x > 20 & x <= 25 ~ 3,
                    x > 25 ~ 4,
                    TRUE ~ x)} )
      
      class_1 <- dplyr::full_join(dplyr::select(class_1, -var_b), b)
    }
    
    # 3.  NT_X
    if(sum(names(to_graph) == 'NT_X') > 0){
      
      b <- to_graph %>%  
        dplyr::select(id, time1, NT_X) %>% 
        dplyr::mutate(NT_X =case_when(NT_X <= 0 ~ 1, 
                                      NT_X > 0 & NT_X <= 5~ 2, 
                                      NT_X > 5 & NT_X <= 10~ 3, 
                                      NT_X > 10 ~ 4, 
                                      TRUE ~ NT_X) )
      class_1 <- dplyr::full_join(class_1 %>% dplyr::select(-NT_X), b)
    }
    
    # 4. 'NWLD','NWLD50','NWLD90'
    if(sum(names(to_graph) %in% c('NWLD','NWLD50','NWLD90', 'NWLD_max','NWLD50_max','NWLD90_max')) > 0){
      var_b <- names(to_graph)[names(to_graph) %in% c('NWLD','NWLD50','NWLD90', 'NWLD_max','NWLD50_max','NWLD90_max')]
      b <- to_graph %>%
        dplyr::select(id, time1, var_b) %>% 
        dplyr::mutate_at(.vars = var_b, .funs = function(x){
          case_when(x <= 2 ~ 1, 
                    x > 2 & x <= 5 ~ 2,
                    x > 5 & x <= 8 ~ 3,
                    x > 8 ~ 4,
                    TRUE ~ x)} )
      
      class_1 <- dplyr::full_join(dplyr::select(class_1, -var_b), b)
    }
    
    # 5. 'LGP'
    if(sum(names(to_graph) == 'LGP') > 0){
      
      b <- to_graph %>%  
        dplyr::select(id, time1, LGP) %>% 
        dplyr::mutate(LGP =case_when(LGP <= 40 ~ 1, 
                                     LGP > 40 & LGP <= 60~ 2, 
                                     LGP > 60 & LGP <= 90~ 3, 
                                     LGP > 90 ~ 4, 
                                     TRUE ~ LGP) )
      class_1 <- dplyr::full_join(class_1 %>% dplyr::select(-LGP), b)
    }
    
    # 6. SLGP_CV  
    if(sum(names(to_graph) == 'SLGP_CV') > 0){
      
      b <- to_graph %>%  
        dplyr::select(id, time1, SLGP_CV) %>% 
        dplyr::mutate(SLGP_CV =case_when(SLGP_CV <= 5 ~ 1, 
                                     SLGP_CV > 5 & SLGP_CV <= 15~ 2, 
                                     SLGP_CV > 15 & SLGP_CV <= 30~ 3, 
                                     SLGP_CV > 30 ~ 4, 
                                     TRUE ~ SLGP_CV) )
      class_1 <- dplyr::full_join(class_1 %>% dplyr::select(-SLGP_CV), b)
    }
    
    
    # Aqui hay que hacer algunos cambios. Ya que la variable 
    # de tipo 23 se construyo anteriormente. 
    # 7. 'THI_0', 'THI_1', 'THI_2', 'THI_3', 'HSI_0', 'HSI_1', 'HSI_2', 'HSI_3', 'SHI'
    if(sum(names(to_graph) %in% c('THI_0', 'THI_1', 'THI_2', 'THI_3', 'THI_23', 'HSI_0', 'HSI_1', 'HSI_2', 'HSI_3', 'HSI_23', 'SHI')) > 0){
      
      var_b <- names(to_graph)[names(to_graph) %in% c('THI_0', 'THI_1', 'THI_2', 'THI_3', 'THI_23', 'HSI_0', 'HSI_1', 'HSI_2', 'HSI_3', 'HSI_23', 'SHI')]
      b <- to_graph %>%
        dplyr::select(id, time1, var_b) %>% 
        dplyr::mutate_at(.vars = var_b, .funs = function(x){
          case_when(x <= 0.25 ~ 1, 
                    x > 0.25 & x <= 0.5 ~ 2,
                    x > 0.5 & x <= 0.75 ~ 3,
                    x > 0.75 ~ 4,
                    TRUE ~ x)} )
      
      class_1 <- dplyr::full_join(dplyr::select(class_1, -var_b), b)
    }
    
    # 8. SPI
    if(sum(names(to_graph) == 'SPI') > 0){
      
      b <- to_graph %>%
        dplyr::select(id, time1, SPI) %>% 
        dplyr::mutate(SPI =case_when(SPI <= 25 ~ 1, 
                                     SPI > 25 & SPI <= 50~ 2, 
                                     SPI > 50 & SPI <= 75~ 3, 
                                     SPI > 75 ~ 4, 
                                     TRUE ~ SPI) )
      class_1 <- dplyr::full_join(class_1 %>% dplyr::select(-SPI), b)
    }
    
    # =----- 
    fix_Vars <- var_toG[var_toG %in% c('SPI','TAI', 'NDWS', 'NDD', 'NT_X', 'NWLD','NWLD50','NWLD90', 'LGP', 'THI_0', 'THI_1', 'THI_2', 'THI_3', 'HSI_0', 'HSI_1', 'HSI_2', 'HSI_3', 'SHI', 'SLGP_CV', 'THI_23', 'HSI_23', 'NWLD_max','NWLD50_max','NWLD90_max')]
    
    
    # fixed categorical class maps. 
    for(i in 1:length(fix_Vars)){
      
      labs_qq <- case_when('1' = fix_Vars[i] %in%  c('TAI', 'NDWS', 'NDD', 'NT_X', 'NWLD','NWLD50','NWLD90', 'NWLD_max','NWLD50_max','NWLD90_max') ~ c('No significant stress', 'Moderate stress', 'Severe','Extreme'),
                           '2' = fix_Vars[i] %in% c('LGP') ~ c('Very low','Low','Moderate','High'),
                           '3' = fix_Vars[i] %in% c('SLGP_CV') ~ c('Low','Moderate','High','Extreme'),
                           '4' = fix_Vars[i] %in% c('THI_0', 'THI_1', 'THI_2', 'THI_3', 'THI_23', 'HSI_0', 'HSI_1', 'HSI_2', 'HSI_3', 'HSI_23', 'SHI') ~ c('Low','Moderate','High','Very high'), 
                           '5' = fix_Vars[i] == 'SPI' ~ c('Limited','Significant','Substantial','Very large'))
      
      
      
      pattern <- case_when(
        fix_Vars[i] == "TAI" ~ 'TAI\nAridity',
        fix_Vars[i] == "LGP" ~ 'LGP\n(days)', 
        fix_Vars[i] == 'NDD' ~ 'NDD\n(days/month)', 
        fix_Vars[i] == 'NDWS' ~ 'NDWS\n(days/month)', 
        fix_Vars[i] == 'NT_X' ~ 'NT_X\n(days/month)', 
        fix_Vars[i] == 'NWLD' ~ 'NWLD\n(days/month)', 
        fix_Vars[i] == 'NWLD50' ~ 'NWLD50\n(days/month)', 
        fix_Vars[i] == 'NWLD90' ~ 'NWLD90\n(days/month)', 
        fix_Vars[i] == 'SHI' ~ 'SHI\n(prob)',
        fix_Vars[i] == 'HSI_0' ~ 'HSI_0\n(prob)', 
        fix_Vars[i] == 'HSI_1' ~ 'HSI_1\n(prob)',
        fix_Vars[i] == 'HSI_2' ~ 'HSI_2\n(prob)', 
        fix_Vars[i] == 'HSI_3' ~ 'HSI_3\n(prob)',
        fix_Vars[i] == 'THI_0' ~ 'THI_0\n(prob)', 
        fix_Vars[i] == 'THI_1' ~ 'THI_1\n(prob)',
        fix_Vars[i] == 'THI_2' ~ 'THI_2\n(prob)', 
        fix_Vars[i] == 'THI_3' ~ 'THI_3\n(prob)',
        fix_Vars[i] == 'SLGP_CV' ~ 'SLGP_CV\n(%)',
        var_toG[i] == 'SPI' ~ 'SPI\n(% area)',
        fix_Vars[i] == 'THI_23' ~ 'THI_2 + THI_3\n(prob)',
        fix_Vars[i] == 'HSI_23' ~ 'HSI_2 + HSI_3\n(prob)',
        var_toG[i] == 'NWLD_max' ~ 'NWLD_max\n(days/month)', 
        var_toG[i] == 'NWLD50_max' ~ 'NWLD50_max\n(days/month)', 
        var_toG[i] == 'NWLD90_max' ~ 'NWLD90_max\n(days/month)',
        TRUE ~ fix_Vars[i])
      
      ggplot() +
        geom_tile(data =  tidyr::drop_na(class_1, !!rlang::sym(fix_Vars[i]) ), aes(x = x, y = y, fill = !!rlang::sym(fix_Vars[i]) %>% as.factor(.) )) +
        geom_sf(data = glwd1, fill = 'lightblue', color = 'lightblue') +
        geom_sf(data = glwd2, fill = 'lightblue', color = 'lightblue') +
        geom_sf(data = ctn, fill = NA, color = gray(.5)) +
        # geom_sf(data = shp_sf, fill = NA, color = gray(.1)) +
        geom_sf(data = zone, fill = NA, color = 'black') +
        scale_fill_manual(values = c( '1' = "#A7D96A", '2' = "#FFFFC1", '3' = "#FDAE61", '4' = '#D7191B', '5' = "#533104"), 
                          breaks = c('1', '2', '3', '4'), labels = labs_qq) +
        labs(fill = pattern, x = NULL, y = NULL, title = title) +
        scale_y_continuous(breaks = round(ylims, 2), n.breaks = 3) +
        scale_x_continuous(breaks = round(xlims, 2), n.breaks = 3) +
        coord_sf(xlim = xlims, ylim = ylims) +
        facet_grid(~time1, labeller = as_labeller( c('1' = "Historic", '2' = "2021-2040", '3' = '2041-2060'))) +
        theme_bw() +
        guides(fill=guide_legend(nrow=2,byrow=TRUE)) +
        theme(legend.position = 'bottom', text = element_text(size=35),
              axis.text.x=element_blank(), axis.text.y=element_blank(),
              strip.background = element_rect(colour = "black", fill = "white"),
              legend.title=element_text(size=35),
              legend.spacing = unit(5, units = 'cm'),
              legend.spacing.x = unit(1.0, 'cm'), plot.title = element_text(hjust = 0.5))
      
      ggsave(glue::glue('{path}/Fix_{fix_Vars[i]}.png') , width = 15, height = 10, dpi = 300)
    }
    
    
    # =--------------------------------------------------
    # Overlays variables. 
    
    group_H <- tibble(vars = c('AMT', 'ATR', 'SPI', 'TAI', 'NDWS', 'NDD', 'NT_X', 'THI_0', 'THI_1', 'THI_2', 'THI_3',	'HSI_0', 'HSI_1', 'HSI_2','HSI_3','SHI', 'NWLD','NWLD50', 'NWLD90',	'IRR', 'LGP', 'SLGP', 'P5D', 'P95', 'SLGP_CV'), 
                      group = c('No', 'No', 'Drought', 'Drought', 'Drought', 'Drought', 'Heat', 'No','No', 'Heat', 'Heat', 'No', 'No', 'Heat', 	'Heat', 'Heat','Waterlogging & flooding', 'Waterlogging & flooding', 'Waterlogging & flooding','Drought', 'Agricultural productivity', 'No', 'Waterlogging & flooding', 'Waterlogging & flooding','Agricultural productivity' ))
    
    high_lvl <- c('SPI', 'TAI','NDWS', 'NDD','NT_X','THI_2','THI_3','HSI_2','HSI_3','SHI','NWLD','NWLD50', 'NWLD90','IRR','P5D','P95', 'SLGP_CV')
    low_lvl <- 'LGP'
    
    final_groups <- group_H %>% dplyr::filter(group  != 'No') %>% dplyr::filter(vars %in% var_toG)
    
    if(sum(dplyr::pull(final_groups, vars) == 'HSI_2') > 0){
      final_groups <- dplyr::filter(final_groups, vars %in% c('HSI_2', 'HSI_3')) %>% 
        dplyr::slice(1) %>% dplyr::mutate(vars = 'HSI_23') %>% 
        dplyr::bind_rows(dplyr::filter(final_groups, !(vars %in% c('HSI_2', 'HSI_3'))),.)
      
      high_lvl <-  c(high_lvl[-which(high_lvl %in%  c('HSI_2', 'HSI_3'))], 'HSI_23')
      
    }
    
    if(sum(dplyr::pull(final_groups, vars) == 'THI_2') > 0){
      final_groups <- dplyr::filter(final_groups, vars %in% c('THI_2', 'THI_3')) %>% 
        dplyr::slice(1) %>% dplyr::mutate(vars = 'THI_23') %>% 
        dplyr::bind_rows(dplyr::filter(final_groups, !(vars %in% c('THI_2', 'THI_3'))),.)
      
      high_lvl <-  c(high_lvl[-which(high_lvl %in%  c('THI_2', 'THI_3'))], 'THI_23')
    }
    
    if(sum(dplyr::pull(final_groups, vars) == 'NWLD')){
      final_groups <- dplyr::filter(final_groups, vars %in% c('NWLD', 'NWLD50', 'NWLD90')) %>%
        dplyr::mutate(vars = c('NWLD_max', 'NWLD50_max', 'NWLD90_max')) %>%
        dplyr::bind_rows(final_groups, .)
      
      high_lvl <-  c(high_lvl, 'NWLD_max', 'NWLD50_max', 'NWLD90_max')
      
    }
    
    # =-----
    
    
    # =- Low level var reclassify
    vars_lvl <- names(class_1)[names(class_1) %in% low_lvl] 
    if(length(low_lvl) > 0){
      class_1 <- class_1 %>% 
        dplyr::mutate_at(.vars = vars_lvl, .funs = function(x){ifelse(x %in% 1:2, 1, 0)})
    }
    # =- High level var reclassify
    vars_lvl <- names(class_1)[names(class_1) %in% high_lvl] 
    if(length(vars_lvl) > 0){
      class_1 <- class_1 %>% 
        dplyr::mutate_at(.vars = vars_lvl, .funs = function(x){ifelse(x %in% 3:4, 1, 0)})
    }
    
    class_1 <- class_1 %>% unique() %>% 
      dplyr::select(id,x,y,time,time1, final_groups$vars)
    
    # =-----------------------------------------------
    
    groups <- dplyr::count(final_groups,group) 
    
    all_Hz <- groups %>% dplyr::select(group) %>% 
      dplyr::group_split(group) %>% 
      purrr::map(.f =  function(x){ x <- x$group
      z <- str_split( x, ' ')[[1]][1]
      dat_mod <- class_1  %>%
        dplyr::select(id,time1, dplyr::filter(final_groups, group == x)$vars) %>% 
        dplyr::rowwise() %>% 
        dplyr::mutate(x = sum(c_across(dplyr::filter(final_groups, group == x)$vars), na.rm = T) %>% round(. , 0)) %>% 
        dplyr::select( id, time1,  x ) %>% dplyr::ungroup() %>% 
        unique() %>%  setNames(c('id', 'time1', z))
      return(dat_mod)})
    
    all_Hz <- all_Hz %>% purrr::reduce(.f = dplyr::inner_join) %>%
      dplyr::inner_join( coord, .)
    
    
    # Drought - Heat - Waterlogging - Agricultural 
    
    all_Hz <- all_Hz %>%
      dplyr::mutate(Dr_Ha = glue::glue('{Drought}-{Heat}'), 
                    Dr_Wa = glue::glue('{Drought}-{Waterlogging }'), 
                    Ha_Wa = glue::glue('{Heat}-{Waterlogging}'))
    
    path_raster <- glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/results/maps/{R_zone}_{season}/tif')
    dir.create(path_raster,recursive = TRUE) 
    write_fst(all_Hz ,  glue::glue('{path_raster}/Final.fst'))
    
    rasterize_mod <- function(ind, time_t, all_Hz = all_Hz){
      
      df <- all_Hz %>% dplyr::filter(time1 == time_t) %>%
        dplyr::select(x, y, ind)
      
      dfr <- raster::rasterFromXYZ(df)
      raster::writeRaster(dfr, filename = glue::glue('{path_raster}/{ind}_{time_t}.tif'))
    }
    
    tibble::tibble(ind = rep(c('Drought','Heat', 'Waterlogging'), each = 3), 
                   time_t = rep(1:3, times = 3) ) %>% 
      dplyr::mutate(raster = purrr::map2(.x = ind, .y = time_t, .f = rasterize_mod,  all_Hz = all_Hz))
    
    # =-----------------
    # Univariate maps. 
    uni_vars <- names(all_Hz)[which(names(all_Hz) %in% c('Agricultural', 'Drought',  'Heat', 'Waterlogging' ))]
    
    for(i in 1:length(uni_vars)){
      
      pattern <- case_when(
        uni_vars[i] == "Agricultural" ~ 'Agricultural productivity',
        uni_vars[i] == "Drought" ~ 'Drought', 
        uni_vars[i] == 'Heat' ~ 'Heat Stress', 
        uni_vars[i] == 'Waterlogging' ~ 'Waterlogging / flooding')
      
      
      unv <- all_Hz %>% dplyr::select(x, y, time1, uni_vars[i]) %>% 
        dplyr::rename(ind = uni_vars[i]) %>% dplyr::mutate(ind = as.factor(ind))
      
      ggplot2::ggplot() +
        ggplot2::geom_tile(data = unv, aes(x = x, y = y, fill = ind) ) +
        geom_sf(data = glwd1, fill = 'lightblue', color = 'lightblue') +
        geom_sf(data = glwd2, fill = 'lightblue', color = 'lightblue') +
        geom_sf(data = ctn, fill = NA, color = gray(.5)) +
        # geom_sf(data = shp_sf, fill = NA, color = gray(.1)) +
        geom_sf(data = zone, fill = NA, color = 'black') +      
        ggplot2::scale_fill_brewer(palette = 'PuBuGn') +
        coord_sf(xlim = xlims, ylim = ylims) +
        ggplot2::theme_bw() +
        ggplot2::xlab('') +
        ggplot2::ylab('') +
        ggplot2::labs(fill = pattern) +
        ggplot2::facet_wrap(~time1, labeller = as_labeller( c('1' = "Historic", '2' = "2021-2040", '3' = '2041-2060')) ) +
        ggplot2::theme(legend.position = 'bottom',
                       axis.text        = element_blank(),
                       text = element_text(size=35),
                       strip.text.x     = element_text(size = 35),
                       strip.background = element_rect(colour = "black", fill = "white"))
      
      ggsave(glue::glue('{path}/Uni_{uni_vars[i]}.png') , width = 15, height = 10, dpi = 300)
    }
    
    
    
    # =-------------------------------
    colmat<-  function(nquantiles=10, upperleft=rgb(0,150,235, maxColorValue=255), upperright=rgb(130,0,80, maxColorValue=255), bottomleft="grey", bottomright=rgb(255,230,15, maxColorValue=255), xlab="x label", ylab="y label"){
      
      my.data<-seq(0,1,.01)
      my.class<-classInt::classIntervals(my.data,n=nquantiles,style="quantile")
      my.pal.1<-classInt::findColours(my.class,c(upperleft,bottomleft))
      my.pal.2<-classInt::findColours(my.class,c(upperright, bottomright))
      col.matrix<-matrix(nrow = 101, ncol = 101, NA)
      
      for(i in 1:101){
        my.col<-c(paste(my.pal.1[i]),paste(my.pal.2[i]))
        col.matrix[102-i,]<-classInt::findColours(my.class,my.col)}
      plot(c(1,1),pch=19,col=my.pal.1, cex=0.5,xlim=c(0,1),ylim=c(0,1),frame.plot=F, xlab=xlab, ylab=ylab,cex.lab=1.3)
      
      for(i in 1:101){
        col.temp<-col.matrix[i-1,]
        points(my.data,rep((i-1)/100,101),pch=15,col=col.temp, cex=1)}
      
      seqs<-seq(0,100,(100/nquantiles))
      seqs[1]<-1
      col.matrix<-col.matrix[c(seqs), c(seqs)]}
    
    # =-------------------------------
    
    
    # =----------------
    # Bivariate maps. 
    final_map <- function(lab_t, all_Hz){
      
      st_hz <- dplyr::select(all_Hz, lab_t) %>% dplyr::pull(.)
      tbl <- all_Hz %>% dplyr::select(id, x, y, time1, lab_t) %>% 
        setNames(c('id', 'x', 'y', 'time1', 'var'))
      
      tst <- data.frame(var = sort( unique(st_hz) ))
      
      mx_lvl <- base::strsplit(tst$var, '-') %>% unlist %>% as.numeric() %>% max()
      
      col.matrix<- colmat(nquantiles = mx_lvl +1 , upperleft="#64ACBE", upperright="#574249", bottomleft="#F5EEF8", bottomright="#C85A5A", xlab='', ylab = '') 
      col.matrix <- col.matrix[-1, ][, -1]
      colnames(col.matrix) = glue::glue('{0:mx_lvl}')
      rownames(col.matrix) = glue::glue('{0:mx_lvl}')
      
      
      col.matrix <- col.matrix %>% as.table() %>% as.data.frame() %>% 
        dplyr::mutate(var = glue::glue('{Var1}-{Var2}')) %>% 
        dplyr::rename(col = 'Freq')
      
      tst <- dplyr::inner_join(tst, dplyr::select(col.matrix, var, col) )
      tbl <- dplyr::left_join(x = tbl, y = tst, by = 'var')
      
      dat <- ggplot2::ggplot() +
        ggplot2::geom_tile(data = tbl, aes(x = x, y = y), fill = tbl$col, alpha = 0.8) +
        geom_sf(data = glwd1, fill = 'lightblue', color = 'lightblue') +
        geom_sf(data = glwd2, fill = 'lightblue', color = 'lightblue') +
        geom_sf(data = ctn, fill = NA, color = gray(.5)) +
        # geom_sf(data = shp_sf, fill = NA, color = gray(.1)) +
        geom_sf(data = zone, fill = NA, color = 'black') +      ggplot2::scale_fill_brewer(palette = 'PuRd') +
        coord_sf(xlim = xlims, ylim = ylims) +
        ggplot2::theme_bw() +
        ggplot2::xlab('') +
        ggplot2::ylab('') +
        ggplot2::facet_wrap(~time1, labeller = as_labeller( c('1' = "Historic", '2' = "2021-2040", '3' = '2041-2060')) ) +
        ggplot2::labs(fill = 'Count') +
        ggplot2::theme(legend.position = 'bottom',
                       axis.text        = element_blank(),
                       text = element_text(size=35),
                       strip.text.x     = element_text(size = 35),
                       strip.background = element_rect(colour = "black", fill = "white"))
      
      
      # =--- 
      oth_tst <- dplyr::select(col.matrix, var) %>% dplyr::pull()
      col.matrix$x <- as.factor(substr(x = oth_tst, start=1, stop=1))
      col.matrix$y <- as.factor(substr(x = oth_tst, start=3, stop=3))
      
      
      leg <-  col.matrix %>% ggplot2::ggplot(aes(x = x, y = y)) +
        ggplot2::geom_tile(fill = col.matrix$col) +
        ggplot2::coord_equal() +
        ggplot2::theme_minimal() +
        # scale_x_discrete() + scale_y_discrete() +
        ggplot2::theme(axis.text       = element_text(size = 35),
                       axis.title      = element_text(size = 20),
                       legend.text     = element_text(size = 17),
                       legend.title    = element_blank(),
                       plot.title      = element_text(size = 25),
                       plot.subtitle   = element_text(size = 17),
                       strip.text.x    = element_text(size = 17),
                       plot.caption    = element_text(size = 15, hjust = 0),
                       legend.position = "bottom") 
      
      
      if(lab_t == 'Dr_Ha'){
        leg <- leg + ggplot2::xlab(expression('Drought' %->% '')) +
          ggplot2::ylab(expression('Heat stress' %->% ''))
      }
      if(lab_t == 'Dr_Wa'){
        leg <- leg + ggplot2::xlab(expression('Drought' %->% '')) +
          ggplot2::ylab(expression('Waterlogging / flooding' %->% ''))
      }
      if(lab_t == 'Ha_Wa'){
        leg <- leg + ggplot2::xlab(expression('Heat stress' %->% '')) +
          ggplot2::ylab(expression('Waterlogging / flooding' %->% ''))
      }
      
      
      png(filename = glue::glue('{path}/Bi_{lab_t}.png'), width=28,height=10,units="in", res = 300)
      print(gridExtra::grid.arrange(leg, dat, nrow = 2, layout_matrix = rbind(c(NA,2, 2, 2, 2, 2),
                                                                              c(1,2, 2, 2, 2, 2))) )
      dev.off()
      
    }
    
    # tictoc::tic()
    final_map(lab_t = 'Dr_Ha', all_Hz = all_Hz)
    final_map(lab_t = 'Dr_Wa', all_Hz = all_Hz)
    final_map(lab_t = 'Ha_Wa', all_Hz = all_Hz)
    # tictoc::toc()
    
    
    # =---------------------------------
    # Location 
    
    # =--------------------------------------------
    # Limits 
    xlims <<- sf::st_bbox(shp_sf)[c(1, 3)]
    ylims <<- sf::st_bbox(shp_sf)[c(2, 4)]
    
    b <- ggplot() +
      geom_sf(data = ctn,  fill = '#AEB6BF', color = gray(.1)) +
      geom_sf(data = shp_sf,  fill = '#D5DBDB', color = gray(.1)) +
      geom_sf(data = zone, aes(fill = Short_Name), color = gray(.1)) +
      geom_sf(data = glwd1, fill = 'lightblue', color = 'lightblue') +
      geom_sf(data = glwd2, fill = 'lightblue', color = 'lightblue') +
      coord_sf(xlim = xlims, ylim = ylims) +
      scale_fill_brewer(palette = "Set3") +
      labs(x = NULL, y = NULL, fill = NULL) +
      theme_bw() +
      theme(legend.position = 'bottom', 
            text = element_text(size=18), 
            axis.text        = element_blank(),
            legend.text = element_text(size=18),
            legend.title=element_text(size=18))  +  
      guides(fill = guide_legend(ncol = 1))
    
    ggsave(glue::glue('{path}/Location.png') , width = 8, height = 5.5, dpi = 300)
    
    
  }
  
  
  for(i in 1:length(seasons)){
    mapping_g(R_zone = Zone, iso3 =  iso3, country = country, Period = seasons[i], data_cons = data_cons, coord = coord)
  }
  
}
# Aqui para correr. 
# map_graphs(iso3, country, seasons)