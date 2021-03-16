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
time_series_plot <- function(country = 'Haiti', iso = 'HTI'){
  
  # Load packages
  if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
  suppressMessages(pacman::p_load(tidyverse,fst))
  
  root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'
  
  # Load historical time series
  pst <- fst::read_fst(paste0(root,"/7.Results/",country,"/past/",iso,"_indices.fst"))
  pst$gSeason <- pst$SLGP <- pst$LGP <- NULL
  fut_fls <- paste0(root,'/7.Results/',country,'/future') %>%
    list.files(., pattern = paste0(iso,'_indices.fst'), full.names = T, recursive  = T)
  if(length(fut_fls) > 0){
    fut <- fut_fls %>% purrr::map(.f = function(f){fst::read_fst(f)}) %>% dplyr::bind_rows()
    fut$gSeason <- fut$SLGP <- fut$LGP <- NULL
    fut <- fut %>% dplyr::group_by(id, season, year) %>% dplyr::summarise_all(median, na.rm = T)
  }
  ifelse(exists('fut'), tbl <- rbind(pst,fut), tbl <- pst)
  tbl$NDD  <- tbl$NDD/7
  tbl$NT_X <- tbl$NT_X/7
  tbl$NDWS <- tbl$NDWS/7
  tbl$NWLD <- tbl$NWLD/7
  tbl$NWLD50 <- tbl$NWLD50/7
  tbl$NWLD90 <- tbl$NWLD90/7
  
  # Obtain number of seasons
  seasons <- as.character(unique(tbl$season))
  for(i in 1:length(seasons)){
    tbl_lng <- tbl %>%
      dplyr::select(id:THI_3) %>%
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
        
        df %>%
          ggplot2::ggplot(aes(x = year, y = Value, group = id)) +
          ggplot2::geom_line(alpha = .05, colour = 'gray') +
          ggplot2::theme_bw() +
          ggplot2::stat_summary(fun = median, geom = "line", lwd = 1.2, colour = 'red', aes(group=1)) +
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
          ggplot2::ggsave(paste0(root,'/7.Results/',country,'/results/time_series/',vr,'_',seasons[i],'.jpeg'), device = 'jpeg', width = 10, height = 8, units = 'in', dpi = 350)
        
      })
  }
  
}
