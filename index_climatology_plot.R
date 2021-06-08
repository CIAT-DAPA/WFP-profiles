# WFP Climate Risk Project
# =----------------------
# Bar graphs
# A. Esquivel - H. Achicanoy - C.Saavedra 
# =----------------------
bar_graphs <- function(country, iso, region = 'all'){
  
  path_save <- glue::glue('{root}/7.Results/{country}/results/monthly_g/')
  dir.create(path_save)
  
  
  # Historic indices
  h0 <- paste0(root,'/7.Results/',country,'/past/',iso,'_indices_monthly.fst') %>%
    tidyft::parse_fst(.) %>%
    base::as.data.frame()
  h0$model <- 'Historical'
  h0$season <- gsub(pattern = 's', replacement = '', h0$season)
  h0$period <- '1981-2019'
  h0$time   <- 'Historical'
  
  ff <- list.files(path = paste0(root,'/7.Results/',country,'/future/'), pattern = '.fst$', full.names = T, recursive = T) %>%
    grep(pattern = '_indices_monthly.fst$', x = ., value = T) %>%
    grep(pattern = iso, x = ., value = T)
  if(length(ff) > 0){
    f1 <- ff %>%
      grep(pattern = '2021-2040', x = ., value = T) %>%
      purrr::map(.f = function(m){
        tb <- m %>%
          tidyft::parse_fst(path = .) %>%
          base::as.data.frame()
        tb$model  <- m %>% strsplit(x = ., split = '/') %>% purrr::map(10) %>% unlist()
        tb$period <- '2021-2040'
        tb$season <- gsub(pattern = 's', replacement = '', tb$season)
        tb$time   <- 'Future'
        return(tb)
      }) %>%
      dplyr::bind_rows()
    f2 <- ff %>%
      grep(pattern = '2041-2060', x = ., value = T) %>%
      purrr::map(.f = function(m){
        tb <- m %>%
          tidyft::parse_fst(path = .) %>%
          base::as.data.frame()
        tb$model  <- m %>% strsplit(x = ., split = '/') %>% purrr::map(10) %>% unlist()
        tb$period <- '2041-2060'
        tb$season <- gsub(pattern = 's', replacement = '', tb$season)
        tb$time   <- 'Future'
        return(tb)
      }) %>%
      dplyr::bind_rows()
  }
  
  # Identify intersected pixels
  px <- base::intersect(h0$id, f1$id)
  h0 <- h0[h0$id %in% px,]
  f1 <- f1[f1$id %in% px,]
  f2 <- f2[f2$id %in% px,]
  
  all <- dplyr::bind_rows(h0,f1,f2)
  
  # -------------------------------------------------------------------------- #
  to_do <- readxl::read_excel(glue::glue('{root}/1.Data/regions_ind.xlsx') )%>%
    dplyr::filter(ISO3 == iso) %>%
    dplyr::rename('Livehood_z' = 'Livelihood zones', 'NT_X'= "NT-X")
  
  
  if(region == 'all'){
    var_s <- to_do %>% dplyr::mutate( Regions = 'all', Livehood_z = 'all', Short_Name = 'all') %>% 
      dplyr::mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()}) %>% 
      dplyr::group_by(ISO3, Country, Regions, Livehood_z, Short_Name ) %>% 
      dplyr::summarise_all(. , sum, na.rm = TRUE) %>% ungroup()
    
    title = 'Country'
  }else{
    var_s <- to_do %>% dplyr::filter(Regions == region) %>%
      dplyr::mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()})
    
    title = dplyr::filter(to_do, Regions  == region)$Short_Name   
  }
  
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
  
  if(sum(vars == 'SLGP') == 1){
    vars <- c(vars[vars != 'SLGP'], 'SLGP_CV')}
  
  
  vars <- vars[!(vars %in% c("ISO3","Country","Regions","Livehood_z", "SPI","TAI" ))]
  # -------------------------------------------------------------------------- #
  
  allh <- all[all$time == 'Historical',]
  allf <- all[all$time == 'Future',]
  
  tsth <- allh %>% as_tibble() %>% ungroup() %>% 
    dplyr::select(season,period, year, ATR:THI_3) %>%
    dplyr::distinct() 
  tstf <- allf %>% as_tibble() %>%  ungroup() %>% 
    dplyr::select(season,period,  year, ATR:THI_3) %>%
    dplyr::distinct() 
  
  # =-------------------------
  
  
  
  
  all_data <- dplyr::bind_rows(tsth,tstf) %>%
    as.tibble() %>% 
    dplyr::mutate(THI_23 = THI_2 + THI_3, HSI_23 = HSI_2 + HSI_3) %>% 
    dplyr::group_by(season,period) %>%
    dplyr::summarise_all(~mean(. , na.rm =  TRUE)) %>%
    dplyr::select(-year) %>% 
    dplyr::mutate_at(.vars = vars[vars %in% c('NDD', 'NT_X', 'NDWS', 'NWLD', 'NWLD50', 'NWLD90','SHI')],
                     .funs = ~round(. , 0)) %>% 
    ungroup()
  
  
  all_data_sd <- dplyr::bind_rows(tsth,tstf) %>%
    as.tibble() %>% 
    dplyr::mutate(THI_23 = THI_2 + THI_3, HSI_23 = HSI_2 + HSI_3) %>% 
    dplyr::group_by(season,period) %>%
    dplyr::summarise_all(~sd(. , na.rm =  TRUE)) %>%
    dplyr::select(-year) %>% 
    ungroup()
  
  
  all_data <- dplyr::full_join(tidyr::pivot_longer(all_data, cols= ATR:HSI_23 ,names_to='Index',values_to='Value') ,
                               tidyr::pivot_longer(all_data_sd, cols= ATR:HSI_23 ,names_to='Index',values_to='sd')) 
  
  
  
  p <- all_data %>%
    ggplot2::ggplot(aes(x = factor(season, levels = 1:12), y = Value, fill = period)) +
    ggplot2::geom_bar(stat = "identity", position = position_dodge()) +
    ggplot2::geom_pointrange(aes(ymin=Value-sd, ymax=Value+sd), width=.2, position=position_dodge(.9)) +
    ggplot2::facet_wrap(~Index, scales = 'free') +
    ggplot2::scale_fill_brewer(palette = "Paired") +
    ggplot2::theme_bw() +
    ggplot2::xlab('') + ggplot2::ylab('') +
    ggplot2::theme(axis.text       = element_text(size = 17),
                   axis.title      = element_text(size = 20),
                   legend.text     = element_text(size = 17),
                   legend.title    = element_blank(),
                   plot.title      = element_text(size = 25),
                   plot.subtitle   = element_text(size = 17),
                   strip.text.x    = element_text(size = 17),
                   plot.caption    = element_text(size = 15, hjust = 0),
                   legend.position = "bottom")
  
  ggplot2::ggsave(paste0(path_save, 'climatology_all.jpeg'), device = 'jpeg', width = 24, height = 12, units = 'in', dpi = 350)
  
  
  
  
  # =-------------------------
  complete <- all_data %>% dplyr::filter( Index %in% vars )
  erase <- dplyr::distinct(all_data, Index)$Index[dplyr::distinct(all_data, Index)$Index %in% c(glue::glue('HSI_{2:3}'), glue::glue('THI_{2:3}') )]
  
  complete <- complete %>%
    dplyr::filter(!(Index %in% c("HSI_2","HSI_3","THI_2","THI_3")) ) # Arreglar la siguiente linea.
  
  filt_graphs <- complete %>% 
    ggplot2::ggplot(aes(x = factor(season, levels = 1:12), y = Value, fill = period)) +
    ggplot2::geom_bar(stat = "identity", position = position_dodge()) +
    ggplot2::geom_pointrange(aes(ymin=Value-sd, ymax=Value+sd), width=.2, position=position_dodge(.9)) +
    ggplot2::facet_wrap(~Index, scales = 'free') +
    ggplot2::scale_fill_brewer(palette = "Paired") +
    ggplot2::theme_bw() +
    ggplot2::xlab('') + ggplot2::ylab('') +
    ggplot2::theme(axis.text       = element_text(size = 17),
                   axis.title      = element_text(size = 20),
                   legend.text     = element_text(size = 17),
                   legend.title    = element_blank(),
                   plot.title      = element_text(size = 25),
                   plot.subtitle   = element_text(size = 17),
                   strip.text.x    = element_text(size = 17),
                   plot.caption    = element_text(size = 15, hjust = 0),
                   legend.position = "bottom") 
  
  
  ggplot2::ggsave(paste0(path_save, 'climatology_subset_I.jpeg'), device = 'jpeg', width = 24, height = 12, units = 'in', dpi = 350)
  
  # =-------------------------------
  
  # By regions
  
  shp <- terra::vect(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso),"_regions/",tolower(iso),"_regions.shp"))
  shp$Short_Name <- to_do$Short_Name
  ref <- terra::rast(paste0(root,"/1.Data/chirps-v2.0.2020.01.01.tif")) %>%
    terra::crop(., terra::ext(shp))
  shr <- terra::rasterize(x = shp, y = ref)
  
  allhr <- cbind(terra::extract(x = shr, y = allh[,c('x','y')]),allh)
  allfr <- cbind(terra::extract(x = shr, y = allf[,c('x','y')]),allf)
  
  allhr2 <- allhr %>% tidyr::drop_na()
  allfr2 <- allfr %>% tidyr::drop_na()
  
  tsth <- allhr2 %>% dplyr::select(value,season,period,ATR:THI_3) %>%
    dplyr::distinct() %>%
    as.tibble() %>% dplyr::mutate(THI_23 = THI_2 + THI_3, HSI_23 = HSI_2 + HSI_3) 
  
  tsth$value <- factor(tsth$value)
  levels(tsth$value) <- shp$region
  
  tstf <- allfr2 %>% dplyr::select(value,season,period,ATR:THI_3) %>%
    dplyr::distinct() %>% as.tibble() %>% 
    dplyr::mutate(THI_23 = THI_2 + THI_3, HSI_23 = HSI_2 + HSI_3) 
  tstf$value <- factor(tstf$value)
  levels(tstf$value) <- shp$region
  
  # Guardados... 
  region_mean <- dplyr::bind_rows(tsth,tstf) %>%
    dplyr::group_by(value,season,period) %>%
    dplyr::summarise_all(~mean(. , na.rm =  TRUE)) %>%
    dplyr::mutate_at(.vars = vars[vars %in% c('NDD', 'NT_X', 'NDWS', 'NWLD', 'NWLD50', 'NWLD90','SHI')],
                     .funs = ~round(. , 0)) %>% 
    dplyr::ungroup()
  
  region_sd <- dplyr::bind_rows(tsth,tstf) %>%
    dplyr::group_by(value,season,period) %>% 
    dplyr::summarise_all(~sd(.)) %>%
    dplyr::ungroup()
  
  
  region_data <- dplyr::full_join(tidyr::pivot_longer(region_mean, cols= ATR:HSI_23 ,names_to='Index',values_to='Value') ,
                                  tidyr::pivot_longer(region_sd, cols= ATR:HSI_23 ,names_to='Index',values_to='sd'))
  # =---
  
  region_data %>% 
    dplyr::group_split(Index) %>%
    purrr::map(function(tbl){
      
      mande <- to_do$Short_Name
      # Modificar de acuerdo a lo que se encuentre... sino pues... 
      mande <- str_replace(mande, '-', '\n') 
      mande <- str_replace(mande, ':', '\n') 
      mande <- str_replace(mande, '/', '\n')
      mande <- str_replace(mande, '[[:punct:]]', '\n')
      names(mande) <- to_do$Regions 
      mande_label <- labeller(value = mande)
      
      gg <- tbl %>%
        ggplot2::ggplot(aes(x = factor(season, levels = 1:12), y = Value, fill = period)) +
        ggplot2::geom_bar(stat = "identity", position = position_dodge()) +
        ggplot2::geom_pointrange(aes(ymin=Value-sd, ymax=Value+sd), width=.2, position=position_dodge(.9)) +
        ggplot2::facet_grid(Index~value, labeller = mande_label  ,scales = 'free') +
        ggplot2::scale_fill_brewer(palette = "Paired") +
        ggplot2::theme_bw() +
        ggplot2::xlab('') +
        ggplot2::ylab('') +
        ggplot2::theme(axis.text       = element_text(size = 17),
                       axis.title      = element_text(size = 20),
                       legend.text     = element_text(size = 17),
                       legend.title    = element_blank(),
                       plot.title      = element_text(size = 25),
                       plot.subtitle   = element_text(size = 17),
                       strip.text.x    = element_text(size = 17),
                       strip.text.y    = element_text(size = 17),
                       plot.caption    = element_text(size = 15, hjust = 0),
                       legend.position = "bottom")
      ggplot2::ggsave(filename = paste0(path_save, '/clim_Ind_',unique(tbl$Index),'.jpeg'), plot = gg, device = 'jpeg', width = 20, height = 7, units = 'in', dpi = 350)
      return(print(gg))
    })
  
} 