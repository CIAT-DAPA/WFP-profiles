# WFP Climate Risk Project
# =----------------------
# Tables  
# A. Esquivel - H. Achicanoy - C.Saavedra 
# =----------------------

changes_paths <- function(country, iso){
  # Read all data complete. 
  past <- fst::fst( glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/past/{iso}_indices_monthly.fst') )%>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(time = 'Historic')
  
  gcm <- c('INM-CM5-0', 'ACCESS-ESM1-5', 'EC-Earth3-Veg', 'MPI-ESM1-2-HR', 'MRI-ESM2-0')
  
  #####
  
  # Run the process in parallel for the 30% of the pixels
  ncores <- 5
  plan(cluster, workers = ncores, gc = TRUE)
  
  future <- tibble( file = list.files(glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/{gcm}/'), full.names =  TRUE, recursive = TRUE, pattern = '_indices_monthly') ) %>%
    dplyr::mutate(shot_file = str_remove(file, pattern = glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/future/'))) %>% 
    dplyr::mutate(data = furrr::future_map(.x = file, .f = function(x){x <- fst::fst(x) %>% tibble::as_tibble()})) %>%
    dplyr::select(-file) %>%
    dplyr::mutate(str_split(shot_file, '/') %>% 
                    purrr::map(.f = function(x){tibble(gcm = x[1], time = x[3])}) %>%
                    dplyr::bind_rows()) %>% 
    dplyr::select(gcm, time, data) %>% 
    tidyr::unnest()
  
  future:::ClusterRegistry("stop")
  gc(reset = T)
  
  # =--------
  # if(dir.exists(glue::glue('{root}/7.Results/{country}/data_/past/')) == TRUE){
  dir.create(glue::glue('{root}/7.Results/{country}/data_/past/'), recursive = TRUE)
  # }
  
  for(i in 1:length(gcm)){
    # if(dir.exists(glue::glue('{root}/7.Results/{country}/data_/future/{gcm[i]}/{time[j]}/')) == TRUE){
    dir.create(glue::glue('{root}/7.Results/{country}/data_/future/{gcm[i]}/'), recursive = TRUE)
    # }
  }
  
  # =--------
  past_all <- past %>% dplyr::select(-gSeason, -SLGP, -LGP, -time) %>% 
    unique() %>% 
    dplyr::mutate(THI_23 = THI_2 + THI_3, HSI_23 = HSI_2 + HSI_3) 
  fst::write_fst(x = past_all, path = glue::glue('{root}/7.Results/{country}/data_/past/{iso}_indices_monthly.fst'))
  
  past_gSeason <- past %>% 
    dplyr::select(id, x, y, year, gSeason, SLGP, LGP) %>% 
    unique() 
  fst::write_fst(x = past_gSeason, path = glue::glue('{root}/7.Results/{country}/data_/past/{iso}_indices_gSeason.fst'))
  
  
  future_all <- future %>% dplyr::select(-gSeason, -SLGP, -LGP) %>% 
    unique() %>% 
    dplyr::mutate(THI_23 = THI_2 + THI_3, HSI_23 = HSI_2 + HSI_3)
  
  # =- 
  future_all %>% dplyr::group_split(gcm) %>% 
    purrr::map(.f = function(x){fst::write_fst(x = x, path = glue::glue('{root}/7.Results/{country}/data_/future/{unique(x$gcm)}/{iso}_indices_monthly.fst'))})
  # =-
  
  future_gSeason <- future %>% 
    dplyr::select(gcm, time, id, x, y, year, gSeason, SLGP, LGP) %>% 
    unique() 
  
  # =- 
  future_gSeason %>% dplyr::group_split(gcm) %>% 
    purrr::map(.f = function(x){fst::write_fst(x = x, path = glue::glue('{root}/7.Results/{country}/data_/future/{unique(x$gcm)}/{iso}_indices_gSeason.fst'))})
  # =-
}
