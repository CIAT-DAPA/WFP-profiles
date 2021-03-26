options(warn = -1, scipen = 999)
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse,tidyft))

root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'
country <- 'Burundi'
iso     <- 'BDI'

# Historic indices
h0 <- paste0(root,'/7.Results/',country,'/past/',iso,'_indices_monthly.fst') %>%
  tidyft::parse_fst(.) %>%
  base::as.data.frame()
h0$season <- gsub(pattern = 's', replacement = '', h0$season)
h0$period <- '1981-2019'

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
      tb$period <- '2021-2040'
      tb$season <- gsub(pattern = 's', replacement = '', tb$season)
      return(tb)
    }) %>%
    dplyr::bind_rows()
  f2 <- ff %>%
    grep(pattern = '2041-2060', x = ., value = T) %>%
    purrr::map(.f = function(m){
      tb <- m %>%
        tidyft::parse_fst(path = .) %>%
        base::as.data.frame()
      tb$period <- '2041-2060'
      tb$season <- gsub(pattern = 's', replacement = '', tb$season)
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
to_do <- readxl::read_excel('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/regions_ind.xlsx') %>%
  dplyr::filter(ISO3 == iso) %>%
  dplyr::rename('Livehood_z' = 'Livelihood zones', 'NT_X'= "NT-X")

to_do <- to_do %>% dplyr::filter(ISO3 == iso) %>%
  dplyr::mutate(NWLD50 = NWLD, NWLD90 = NWLD, HSI_0 = HSI, HSI_1 = HSI, HSI_2 = HSI, HSI_3 = HSI,
         THI_0 = THI, THI_1 = THI, THI_2 = THI, THI_3 = THI, SLGP_CV = SLGP ) %>%
  dplyr::select(-THI, -HSI)

region = 'all'

if(region == 'all'){
  var_s <- to_do %>% dplyr::mutate( Regions = 'all', Livehood_z = 'all') %>%
    dplyr::mutate_at(.vars = vars(ATR:SLGP_CV ) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()}) %>%
    dplyr::group_by(ISO3, Country, Regions, Livehood_z) %>%
    dplyr::summarise_all(. , sum, na.rm = TRUE) %>% ungroup()
  title = 'Country'
}else{
  var_s <- to_do %>% filter(Regions == region) %>%
    mutate_at(.vars = vars(ATR:SLGP_CV) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()})
}

var_to <- var_s %>% dplyr::ungroup() %>% dplyr::select(ATR:SLGP_CV) %>%
  tidyr::pivot_longer(cols = dplyr::everything(.), names_to = 'var', values_to = 'count') %>%
  dplyr::mutate(group = c('N/A', 'N/A', 'Drought', 'Drought', 'Drought', 'Drought', 'Drought', 'Agricultural', 'N/A', 'Waterlogging', 'Waterlogging', 'Waterlogging', 'Heat', 'Heat', 'Waterlogging', 'Waterlogging', 'N/A', 'N/A', 'Heat', 'Heat', 'N/A', 'N/A', 'Heat', 'Heat', 'Agricultural' )) %>%
  dplyr::filter(count > 0, group != 'N/A')
# -------------------------------------------------------------------------- #

all %>%
  dplyr::select(season,period,ATR:THI_3) %>%
  tidyr::pivot_longer(cols=ATR:THI_3,names_to='Index',values_to='Value') %>%
  dplyr::group_by(season,period,Index) %>%
  dplyr::summarise(Value = round(mean(Value, na.rm = T))) %>%
  # dplyr::filter(Index %in% var_to$var) %>%
  ggplot2::ggplot(aes(x = factor(season, levels = 1:12), y = Value, fill = period)) +
  ggplot2::geom_bar(stat = "identity", position = position_dodge()) +
  ggplot2::facet_wrap(~Index, scales = 'free') +
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
                 plot.caption    = element_text(size = 15, hjust = 0),
                 legend.position = "bottom") +
  ggplot2::ggsave('D:/climatology_indices.jpeg', device = 'jpeg', width = 24, height = 12, units = 'in', dpi = 350)

all %>%
  dplyr::select(season,period,ATR:THI_3) %>%
  tidyr::pivot_longer(cols=ATR:THI_3,names_to='Index',values_to='Value') %>%
  dplyr::group_by(season,period,Index) %>%
  dplyr::summarise(Value = round(mean(Value, na.rm = T))) %>%
  dplyr::filter(Index %in% var_to$var) %>%
  ggplot2::ggplot(aes(x = factor(season, levels = 1:12), y = Value, fill = period)) +
  ggplot2::geom_bar(stat = "identity", position = position_dodge()) +
  ggplot2::facet_wrap(~Index, scales = 'free') +
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
                 plot.caption    = element_text(size = 15, hjust = 0),
                 legend.position = "bottom") +
  ggplot2::ggsave('D:/climatology_indices_subset.jpeg', device = 'jpeg', width = 24, height = 12, units = 'in', dpi = 350)
