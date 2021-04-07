# -------------------------------------------------- #
# Climate Risk Profiles -- Time series plot
# A. Esquivel & H. Achicanoy
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# Input parameters:
#   iso: ISO 3 code in capital letters
#   country: country name first character in capital letter
# Output:
#   .jpeg graphs per index and season
time_series_plot <- function(country = 'Haiti', iso = 'HTI', seasons){
  
  # Load packages
  if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
  suppressMessages(pacman::p_load(tidyverse,fst))
  
  root   <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'
  outdir <- paste0(root,'/7.Results/',country,'/results/time_series')
  if(!dir.exists(outdir)){ dir.create(outdir, F, T) }
  
  # Load historical time series
  pst <- paste0(root,"/7.Results/",country,"/past/",iso,"_indices.fst") %>%
    tidyft::parse_fst() %>%
    base::as.data.frame()
  pst$gSeason <- pst$SLGP <- pst$LGP <- NULL
  pst$model <- 'Historical'
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
    fut <- fut %>% dplyr::group_by(id, season, model, year) %>% dplyr::summarise_all(median, na.rm = T)
  }
  ifelse(exists('fut'), tbl <- dplyr::bind_rows(pst,fut), tbl <- pst)
  tbl <- tbl %>% tidyr::drop_na()
  ss <- sort(unique(tbl$season))
  for(s in ss){
    dnm <- length(seasons[names(seasons) == s][[1]])
    tbl$NDD[tbl$season == s]  <- tbl$NDD[tbl$season == s]/dnm
    tbl$NT_X[tbl$season == s] <- tbl$NT_X[tbl$season == s]/dnm
    tbl$NDWS[tbl$season == s] <- tbl$NDWS[tbl$season == s]/dnm
    tbl$NWLD[tbl$season == s] <- tbl$NWLD[tbl$season == s]/dnm
    tbl$NWLD50[tbl$season == s] <- tbl$NWLD50[tbl$season == s]/dnm
    tbl$NWLD90[tbl$season == s] <- tbl$NWLD90[tbl$season == s]/dnm
  }; rm(ss, s)
  
  # Obtain number of seasons
  seasons <- as.character(unique(tbl$season))
  for(i in 1:length(seasons)){
    dir.create(path = paste0(outdir,'/all_s',i), F, T)
    tbl_lng <- tbl %>%
      dplyr::select(id:model) %>%
      tidyr::pivot_longer(cols = TAI:THI_3, names_to = 'Indices', values_to = 'Value') %>%
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
        if(vr %in% c(paste0('HSI_',0:3),paste0('THI_',0:3))){ ylb <- 'Probability' }
        if(vr == 'IRR'){ ylb <- '' }
        if(vr %in% c('NDD','NT_X','NDWS','NWLD','NWLD50','NWLD90')){ df <- df %>% tidyr::drop_na(); ylb <- 'days/month' }
        if(vr == 'P5D'){ ylb <- 'mm/5 days' } # df <- df[df$Value < 125,]
        if(vr == 'P95'){ ylb <- 'mm/day' } # df <- df[df$Value < 50,]
        if(vr == 'SHI'){ ylb <- 'days/season' }
        
        if(length(models) > 1){
          df %>%
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
            ggplot2::theme_bw() +
            ggplot2::geom_vline(xintercept = 2020, size = 1, linetype = "dashed", color = "red") +
            ggplot2::theme(axis.text       = element_text(size = 17),
                           axis.title      = element_text(size = 20),
                           legend.text     = element_text(size = 15),
                           legend.title    = element_blank(),
                           plot.title      = element_text(size = 25),
                           plot.subtitle   = element_text(size = 17),
                           strip.text.x    = element_text(size = 17),
                           plot.caption    = element_text(size = 15, hjust = 0),
                           legend.position = "bottom") +
            ggplot2::ggsave(paste0(outdir,'/all_s',i,'/',vr,'.jpeg'), device = 'jpeg', width = 10, height = 8, units = 'in', dpi = 350)
        } else {
          df %>%
            ggplot2::ggplot(aes(x = year, y = Value, group = id)) +
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
                           legend.position = "bottom") +
            ggplot2::ggsave(paste0(outdir,'/all_s',i,'/',vr,'.jpeg'), device = 'jpeg', width = 10, height = 8, units = 'in', dpi = 350)
        }
        
      })
  }
  
}
