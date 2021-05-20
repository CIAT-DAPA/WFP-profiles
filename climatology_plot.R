# -------------------------------------------------- #
# Climate Risk Profiles -- Climatology graph
# H. Achicanoy
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# Input parameters:
#   iso: ISO 3 code in capital letters
#   country: country name first character in capital letter
# Output:
#   .jpeg graph with rainfall pattern and tmin/tmax multi-annual
#   averages (historical and future data (if available))
climatology_plot <- function(country = 'Haiti', iso = 'HTI', output = output){
  
  # Load packages
  if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
  suppressMessages(pacman::p_load(tidyverse,lubridate,fst,tidyft))
  
  OSys <- Sys.info()[1]
  root <<- switch(OSys,
                  'Linux'   = '/dapadfs/workspace_cluster_13/WFP_ClimateRiskPr',
                  'Windows' = '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr')
  
  fut <- paste0(root,'/7.Results/',country,'/future') %>%
    list.files(., pattern = paste0(iso,'_indices.fst'), full.names = T, recursive  = T)
  
  if(length(fut) > 0){
    px_past <- paste0(root,'/7.Results/', country, '/past/', iso,'_indices.fst') %>% 
      tidyft::parse_fst() %>% tidyft::select_fst(id) %>% dplyr::pull() %>% unique()
    
    fut_id <- 1:length(fut) %>% as.list()  %>% 
      purrr::map(.f = function(i){
        df <- fut %>% tidyft::parse_fst() %>% tidyft::select_fst(id) %>% 
          dplyr::pull() %>% unique()
      })  %>% unlist() %>% unique()
    
    # Identify pixels intersection
    px <- base::intersect(px_past, fut_id)
    
    # Generate a random sample of 100 pixels
    if(length(px) > 100){
      set.seed(1235)
      smpl <- sample(x = px, size = 100, replace = F) %>% sort()
    } else {smpl <- px}
    
    pft <<- smpl
    
    # tictoc::tic()
    # List and load future climate
    ff <- list.files(path = paste0(root,'/1.Data/future_data'), pattern = '.fst$', full.names = T, recursive = T) %>%
      grep(pattern = 'bias_corrected', x = ., value = T) %>%
      grep(pattern = iso, x = ., value = T)
    
    ncores <- 5
    plan(cluster, workers = ncores, gc = TRUE)
    
    if(length(ff) > 0){
      f1 <- ff %>%
        grep(pattern = '2021-2040', x = ., value = T) %>%
        furrr::future_map(.f = function(m){
          tb <- m %>%
            tidyft::parse_fst(path = .) %>%
            tidyft::filter_fst(id %in% pft) %>%
            base::as.data.frame()
          tb$model  <- m %>% strsplit(x = ., split = '/') %>% purrr::map(8) %>% unlist()
          tb$period <- '2021-2040'
          return(tb)
        }) %>%
        dplyr::bind_rows()
      f2 <- ff %>%
        grep(pattern = '2041-2060', x = ., value = T) %>%
        furrr::future_map(.f = function(m){
          tb <- m %>%
            tidyft::parse_fst(path = .) %>%
            tidyft::filter_fst(id %in% pft) %>%
            base::as.data.frame()
          tb$model  <- m %>% strsplit(x = ., split = '/') %>% purrr::map(8) %>% unlist()
          tb$period <- '2041-2060'
          return(tb)
        }) %>%
        dplyr::bind_rows()
    }
    future:::ClusterRegistry("stop")
    gc(reset = T)
    # tictoc::toc()
    
  } else {
    px <- paste0(root,'/1.Data/observed_data/',iso,'/year/climate_1981_mod.fst') %>% 
      tidyft::parse_fst() %>% tidyft::select_fst(id) %>% dplyr::pull() %>% unique()
    
    # Generate a random sample of 100 pixels
    if(length(px) > 100){
      set.seed(1235)
      smpl <- sample(x = px, size = 100, replace = F) %>% sort()
    } else {smpl <- px}
    
    pft <<- smpl
  }
  
  # Load historical climate
  h0 <- paste0(root,'/1.Data/observed_data/',iso,'/',iso,'.fst') %>%
    tidyft::parse_fst(path = .) %>%
    tidyft::filter_fst(id %in% pft) %>%
    base::as.data.frame()
  h0$period <- '1981-2019'
  h0$model <- 'Historical'
  
  if(exists('f1') & exists('f2')){
    
    h0$id1 <- h0$id
    h0$year <- NULL
    h0 <- h0 %>%
      tidyr::nest(Climate = c('id','date','prec','tmean','tmax','tmin','srad','wind','rh')) %>%
      dplyr::rename(id = 'id1') %>%
      dplyr::select(id, dplyr::everything(.))
    
    f1$id1 <- f1$id
    f1$year <- NULL
    f1 <- f1 %>%
      tidyr::nest(Climate = c('id','date','prec','tmean','tmax','tmin','srad','wind','rh')) %>%
      dplyr::rename(id = 'id1') %>%
      dplyr::select(id, dplyr::everything(.))
    
    f2$id1 <- f2$id
    f2$year <- NULL
    f2 <- f2 %>%
      tidyr::nest(Climate = c('id','date','prec','tmean','tmax','tmin','srad','wind','rh')) %>%
      dplyr::rename(id = 'id1') %>%
      dplyr::select(id, dplyr::everything(.))
    
    # Identify intersected pixels
    px <- base::intersect(h0$id, as.numeric(names(which(table(f1$id) == 5))))
    px <- base::intersect(px, as.numeric(names(which(table(f2$id) == 5))))
    h0 <- h0[h0$id %in% px,]
    f1 <- f1[f1$id %in% px,]
    f2 <- f2[f2$id %in% px,]
    
    all_clmtlgy <- list(h0, f1, f2) %>%
      purrr::map(.f = function(tbl){
        clmtlgy <- 1:length(px) %>%
          purrr::map(.f = function(i){
            clmtlgy <-  tbl[which(tbl$id == px[i]),] %>% 
              tidyr::unnest() %>% 
              dplyr::mutate(Year  = lubridate::year(lubridate::as_date(date)),
                            Month = lubridate::month(lubridate::as_date(date))) %>%
              dplyr::group_by(Year, Month, model) %>%
              dplyr::summarise(Prec = sum(prec, na.rm = T),
                               Tmin = mean(tmin, na.rm = T),
                               Tmax = mean(tmax, na.rm = T)) %>%
              dplyr::ungroup() %>%
              dplyr::group_by(Month) %>%
              dplyr::summarise(Prec = mean(Prec, na.rm = T),
                               Tmin = mean(Tmin, na.rm = T),
                               Tmax = mean(Tmax, na.rm = T))
            return(clmtlgy)
          }) %>%
          dplyr::bind_rows()
        return(clmtlgy)
      })
    all_clmtlgy[[1]]$period <- '1981-2019'
    all_clmtlgy[[2]]$period <- '2021-2040'
    all_clmtlgy[[3]]$period <- '2041-2060'
    
    avrgs <- all_clmtlgy %>%
      dplyr::bind_rows() %>%
      dplyr::group_by(Month, period) %>%
      dplyr::summarise(Prec = mean(Prec, na.rm = T),
                       Tmin = mean(Tmin, na.rm = T),
                       Tmax = mean(Tmax, na.rm = T))
    
    avgs <- dplyr::inner_join(x = avrgs, y = data.frame(Month = 1:12, month_abb = month.abb), by = 'Month')
    avgs <- avgs %>%
      dplyr::mutate(month_abb = factor(month_abb, levels = month.abb))
    
    pr <- dplyr::pull(avgs, Prec)
    tm <- dplyr::pull(avgs, Tmax)
    rlc <- (mean(pr)/mean(tm) * 2.8)
    
    avgs <- dplyr::inner_join(x = avgs, y = data.frame(period = unique(avgs$period), line_type = c(1,2,3)))
    avgs$line_type <- as.character(avgs$line_type)
    
    gg <- avgs %>% ggplot(aes(x = month_abb)) +
      geom_bar(aes(y = Prec, fill = period), stat = "identity", position = position_dodge()) +
      scale_fill_brewer(palette = "Paired") +
      ylab('Precipitation (mm)') +
      geom_line(data = avgs, aes(y = Tmin*rlc, colour = period, group = period), size = 1.2) +
      geom_line(data = avgs, aes(y = Tmax*rlc, colour = period, group = period), size = 1.2) +
      scale_colour_brewer(palette = "Paired") +
      scale_y_continuous(sec.axis = sec_axis(~./rlc, name = 'Temperature (ºC)')) +
      xlab('') +
      theme_bw() +
      scale_linetype_manual(name = ' ', 
                            values = 1:3, 
                            labels = c("1" = "1981-2019", "2" = "2021-2040", "3" = "2041-2060")) +
      ggplot2::theme(legend.position = 'top',
                     axis.text       = element_text(size = 17),
                     axis.title      = element_text(size = 20),
                     legend.text     = element_text(size = 17),
                     legend.title    = element_blank(),
                     plot.title      = element_text(size = 25),
                     plot.subtitle   = element_text(size = 17),
                     strip.text.x    = element_text(size = 17),
                     plot.caption    = element_text(size = 15, hjust = 0))
    ggplot2::ggsave(output, gg, device = 'png', width = 12, height = 9, units = 'in', dpi = 350)
    
  } else {
    
    px <- smpl
    
    h0$id1 <- h0$id
    h0$year <- NULL
    h0 <- h0 %>%
      tidyr::nest(Climate = c('id','date','prec','tmean','tmax','tmin','srad','wind','rh')) %>%
      dplyr::rename(id = 'id1') %>%
      dplyr::select(id, dplyr::everything(.))
    
    all_clmtlgy <- list(h0) %>%
      purrr::map(.f = function(tbl){
        clmtlgy <- 1:length(px) %>%
          purrr::map(.f = function(i){
            clmtlgy <-  tbl[which(tbl$id == px[i]),] %>% 
              tidyr::unnest() %>% 
              dplyr::mutate(Year  = lubridate::year(lubridate::as_date(date)),
                            Month = lubridate::month(lubridate::as_date(date))) %>%
              dplyr::group_by(Year, Month, model) %>%
              dplyr::summarise(Prec = sum(prec, na.rm = T),
                               Tmin = mean(tmin, na.rm = T),
                               Tmax = mean(tmax, na.rm = T)) %>%
              dplyr::ungroup() %>%
              dplyr::group_by(Month) %>%
              dplyr::summarise(Prec = mean(Prec, na.rm = T),
                               Tmin = mean(Tmin, na.rm = T),
                               Tmax = mean(Tmax, na.rm = T))
            return(clmtlgy)
          }) %>%
          dplyr::bind_rows()
        return(clmtlgy)
      })
    all_clmtlgy[[1]]$period <- '1981-2019'
    
    avrgs <- all_clmtlgy %>%
      dplyr::bind_rows() %>%
      dplyr::group_by(Month, period) %>%
      dplyr::summarise(Prec = mean(Prec, na.rm = T),
                       Tmin = mean(Tmin, na.rm = T),
                       Tmax = mean(Tmax, na.rm = T))
    
    avgs <- dplyr::inner_join(x = avrgs, y = data.frame(Month = 1:12, month_abb = month.abb), by = 'Month')
    avgs <- avgs %>%
      dplyr::mutate(month_abb = factor(month_abb, levels = month.abb))
    
    pr <- dplyr::pull(avgs, Prec)
    tm <- dplyr::pull(avgs, Tmax)
    rlc <- (mean(pr)/mean(tm) * 2.8)
    
    avgs <- dplyr::inner_join(x = avgs, y = data.frame(period = unique(avgs$period), line_type = c(1,2,3)))
    avgs$line_type <- as.character(avgs$line_type)
    
    gg <- avgs %>% ggplot(aes(x = month_abb)) +
      geom_bar(aes(y = Prec, fill = period), stat = "identity", position = position_dodge()) +
      scale_fill_brewer(palette = "Paired") +
      ylab('Precipitation (mm)') +
      geom_line(data = avgs, aes(y = Tmin*rlc, colour = period, group = period), size = 1.2) +
      geom_line(data = avgs, aes(y = Tmax*rlc, colour = period, group = period), size = 1.2) +
      scale_colour_brewer(palette = "Paired") +
      scale_y_continuous(sec.axis = sec_axis(~./rlc, name = 'Temperature (ºC)')) +
      xlab('') +
      theme_bw() +
      scale_linetype_manual(name = ' ', 
                            values = 1:3, 
                            labels = c("1" = "1981-2019", "2" = "2021-2040", "3" = "2041-2060")) +
      ggplot2::theme(legend.position = 'top',
                     axis.text       = element_text(size = 17),
                     axis.title      = element_text(size = 20),
                     legend.text     = element_text(size = 17),
                     legend.title    = element_blank(),
                     plot.title      = element_text(size = 25),
                     plot.subtitle   = element_text(size = 17),
                     strip.text.x    = element_text(size = 17),
                     plot.caption    = element_text(size = 15, hjust = 0))
    ggplot2::ggsave(output, gg, device = 'png', width = 12, height = 9, units = 'in', dpi = 350)
    
  }
  
}
