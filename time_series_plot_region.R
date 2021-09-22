# -------------------------------------------------- #
# Climate Risk Profiles -- Time series plot by region
# A. Esquivel & H. Achicanoy
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

time_series_region <- function(country = 'Haiti', iso = 'HTI', seasons){
  
  # Load packages
  if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
  suppressMessages(pacman::p_load(tidyverse,fst))
  
  # root   <- '//dapadfs.cgiarad.org/workspace_cluster_14/WFP_ClimateRiskPr'
  outdir <- paste0(root,'/7.Results/',country,'/results/time_series')
  if(!dir.exists(outdir)){ dir.create(outdir, F, T) }
  
  shp <- terra::vect(paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_regions/',tolower(iso),'_regions.shp'))
  ref <- terra::rast(paste0(root,"/1.Data/chirps-v2.0.2020.01.01.tif")) %>%
    terra::crop(., terra::ext(shp))
  shr <- terra::rasterize(x = shp, y = ref, field= 'region') %>% setNames(c('value'))
  
  # Load historical time series
  pst <- paste0(root,"/7.Results/",country,"/past/",iso,"_indices.fst") %>%
    tidyft::parse_fst() %>%
    base::as.data.frame()
  pst$gSeason <- pst$SLGP <- pst$LGP <- NULL
  pst$model <- 'Historical'
  pst <- cbind(terra::extract(x = shr, y = pst[,c('x','y')]),pst)
  pst <- pst %>% tidyr::drop_na()
  pst <- pst %>% dplyr::mutate(THI_23 = THI_2 + THI_3, HSI_23 = HSI_2 + HSI_3)
  pst_smm <- pst %>% dplyr::group_by(value, season, year, model) %>% dplyr::summarise_all(median, na.rm = T)
  pst_max <- pst %>% dplyr::group_by(value, season, year, model) %>% dplyr::summarise(NWLD_max = max(NWLD, na.rm = T), NWLD50_max = max(NWLD50, na.rm = T), NWLD90_max = max(NWLD90, na.rm = T))
  pst_smm <- dplyr::left_join(x = pst_smm, y = pst_max, by = c('value', 'season', 'year', 'model')); rm(pst_max)
  pst_smm$id <- pst_smm$x <- pst_smm$y <- NULL
  fut_fls <- paste0(root,'/7.Results/',country,'/future') %>%
    list.files(., pattern = paste0(iso,'_indices.fst'), full.names = T, recursive  = T)
  if(length(fut_fls) > 0){
    models <- fut_fls %>% strsplit(x = ., split = '/') %>% purrr::map(9) %>% unlist()
    fut <- 1:length(fut_fls) %>% purrr::map(.f = function(i){
      df <- fut_fls[i] %>%
        tidyft::parse_fst() %>%
        base::as.data.frame()
      df$model <- models[i]; return(df)
    }) %>% dplyr::bind_rows()
    fut$gSeason <- fut$SLGP <- fut$LGP <- NULL
    fut <- cbind(terra::extract(x = shr, y = fut[,c('x','y')]), fut)
    fut <- fut %>% tidyr::drop_na()
    fut <- fut %>% dplyr::mutate(THI_23 = THI_2 + THI_3, HSI_23 = HSI_2 + HSI_3)
    fut_smm <- fut %>% dplyr::group_by(value, season, year, model) %>% dplyr::summarise_all(median, na.rm = T)
    fut_max <- fut %>% dplyr::group_by(value, season, year, model) %>% dplyr::summarise(NWLD_max = max(NWLD, na.rm = T), NWLD50_max = max(NWLD50, na.rm = T), NWLD90_max = max(NWLD90, na.rm = T))
    fut_smm <- dplyr::left_join(x = fut_smm, y = fut_max, by = c('value', 'season', 'year', 'model')); rm(fut_max)
    fut_smm$id <- fut_smm$x <- fut_smm$y <- NULL
  }
  ifelse(exists('fut_smm'), tbl <- dplyr::bind_rows(pst_smm,fut_smm), tbl <- pst_smm)
  tbl <- tbl %>% tidyr::drop_na()
  tbl$value <- factor(tbl$value)
  #levels(tsth$value) <- sort(unique(shp$region))
  levels(tbl$value) <- sort(unique(shp$region))
  
  ss <- sort(unique(tbl$season))
  for(s in ss){
    dnm <- length(seasons[names(seasons) == s][[1]])
    tbl$NDD[tbl$season == s]  <- tbl$NDD[tbl$season == s]/dnm
    tbl$NT_X[tbl$season == s] <- tbl$NT_X[tbl$season == s]/dnm
    tbl$NDWS[tbl$season == s] <- tbl$NDWS[tbl$season == s]/dnm
    tbl$NWLD[tbl$season == s] <- tbl$NWLD[tbl$season == s]/dnm
    tbl$NWLD50[tbl$season == s] <- tbl$NWLD50[tbl$season == s]/dnm
    tbl$NWLD90[tbl$season == s] <- tbl$NWLD90[tbl$season == s]/dnm
    tbl$NWLD_max[tbl$season == s] <- tbl$NWLD_max[tbl$season == s]/dnm
    tbl$NWLD50_max[tbl$season == s] <- tbl$NWLD50_max[tbl$season == s]/dnm
    tbl$NWLD90_max[tbl$season == s] <- tbl$NWLD90_max[tbl$season == s]/dnm
  }; rm(ss, s)
  
  # Obtain number of seasons
  seasons <- as.character(unique(tbl$season))
  for(i in 1:length(seasons)){
    dir.create(path = paste0(outdir,'/all_s',i,'_lz'), F, T)
    tbl_lng <- tbl %>%
      tidyr::pivot_longer(cols = c(TAI:NWLD90_max), names_to = 'Indices', values_to = 'Value') %>%
      dplyr::ungroup() %>%
      dplyr::group_by(Indices, add = T) %>%
      dplyr::group_split()
    tbl_lng %>%
      purrr::map(.f = function(df){
        df <- unique(df)
        df <- df[df$season == seasons[i],]
        df <- tidyr::drop_na(df)
        vr <- df$Indices %>% unique()
        if(vr == 'AMT'){ ylb <- expression('Temperature ('*~degree*C*')') }
        if(vr == 'ATR'){ ylb <- 'mm/season' }
        if(vr == 'TAI'){ ylb <- 'Aridity' }
        if(vr %in% c(paste0('HSI_',0:3),'HSI_23',paste0('THI_',0:3),'THI_23')){ ylb <- 'Probability' }
        if(vr == 'IRR'){ ylb <- '' }
        if(vr %in% c('NDD','NT_X','NDWS','NWLD','NWLD50','NWLD90','NWLD_max','NWLD50_max','NWLD90_max')){ df <- df %>% tidyr::drop_na(); ylb <- 'days/month' }
        if(vr == 'P5D'){ ylb <- 'mm/5 days' } # df <- df[df$Value < 125,]
        if(vr == 'P95'){ ylb <- 'mm/day' } # df <- df[df$Value < 50,]
        if(vr == 'SHI'){ ylb <- 'days/season' }
        if(vr == 'CSDI'){ ylb <- 'days' }
        
        if(length(models) > 1){
          
          to_do <- readxl::read_excel(glue::glue('{root}/1.Data/regions_ind.xlsx')) %>%
            dplyr::filter(ISO3 == iso) %>%
            dplyr::rename('Livehood_z' = 'Livelihood zones', 'NT_X'= "NT-X")
          
          mande <- to_do$Short_Name
          # Modificar de acuerdo a lo que se encuentre... sino pues... 
          mande <- str_replace(mande, '-', '\n')
          mande <- str_replace(mande, ':', '\n')
          mande <- str_replace(mande, '/', '\n')
          # mande <- str_replace(mande, '[[:punct:]]', '\n')
          names(mande) <- to_do$Regions
          mande_label <- labeller(value = mande)
          
          gg <- df %>%
            dplyr::filter(model == 'Historical') %>% 
            ggplot2::ggplot(aes(x = year, y = Value, group = model)) +
            # ggplot2::geom_line(alpha = .05, colour = 'gray') +
            ggplot2::theme_bw() +
            ggplot2::stat_summary(fun = median, geom = "line", lwd = 1.2, colour = 'black', aes(group=1)) +
            ggplot2::stat_summary(data = df[df$model!='Historical',], fun = median, geom = "line", lwd = 1.2, colour = 'black', aes(group=1)) +
            ggplot2::stat_summary(data = df[df$model!='Historical',] %>% dplyr::group_by(model), fun = median, geom = "line", lwd = 0.5, aes(colour = model)) +
            ggplot2::scale_color_brewer(palette = 'Set1') +
            ggplot2::ggtitle(paste0(vr,': ',seasons[i])) +
            ggplot2::xlab('Year') +
            ggplot2::ylab(ylb) +
            ggplot2::geom_vline(xintercept = 2020, size = 1, linetype = "dashed", color = "red") +
            ggplot2::facet_grid(Indices~value, labeller = mande_label,scales = 'free') +
            ggplot2::theme(axis.text       = element_text(size = 16),
                           axis.title      = element_text(size = 20),
                           legend.text     = element_text(size = 15),
                           legend.title    = element_blank(),
                           plot.title      = element_text(size = 25),
                           plot.subtitle   = element_text(size = 17),
                           strip.text.x    = element_text(size = 17),
                           strip.text.y    = element_text(size = 17),
                           plot.caption    = element_text(size = 15, hjust = 0),
                           legend.position = "bottom")
          ggplot2::ggsave(filename = paste0(outdir,'/all_s',i,'_lz/',vr,'_lz.png'), plot = gg, device = 'jpeg', width = 21, height = 8, units = 'in', dpi = 350)
        } else {
          gg <- df %>%
            ggplot2::ggplot(aes(x = year, y = Value, group = model)) +
            ggplot2::geom_line(alpha = .05, colour = 'gray') +
            ggplot2::theme_bw() +
            ggplot2::stat_summary(fun = median, geom = "line", lwd = 1.2, colour = 'black', aes(group=1)) +
            ggplot2::ggtitle(paste0(vr,': ',seasons[i])) +
            ggplot2::xlab('Year') +
            ggplot2::ylab(ylb) +
            ggplot2::theme_bw() +
            ggplot2::geom_vline(xintercept = 2020, size = 1, linetype = "dashed", color = "blue") +
            ggplot2::theme(axis.text       = element_text(size = 17),
                           axis.title      = element_text(size = 20),
                           legend.text     = element_text(size = 17),
                           legend.title    = element_blank(),
                           plot.title      = element_text(size = 25),
                           plot.subtitle   = element_text(size = 17),
                           strip.text.x    = element_text(size = 17),
                           plot.caption    = element_text(size = 15, hjust = 0),
                           legend.position = "bottom")
          ggplot2::ggsave(filename = paste0(outdir,'/all_s',i,'/',vr,'.png'), plot = gg, device = 'jpeg', width = 10, height = 8, units = 'in', dpi = 350)
        }
        
      })
  }
  
}