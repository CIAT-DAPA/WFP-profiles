# Create slides automatically
# A. Esquivel and H. Achicanoy
# Alliance Bioversity-CIAT, 2020

options(warn = -1, scipen = 999)

suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse, rmarkdown))
library(dplyr)

sld_dir <- 'D:/OneDrive - CGIAR/Desktop/WFP/slides'
region <- 'all'
season <-   1:3 %>% paste0('s', .) %>% as.list()
iso3 <- 'TZA'


create_slides <- function(country = 'Tanzania', season = season, region = region){
  
  
  # Indices...
  
  to_do <- readxl::read_excel('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/regions_ind.xlsx') %>%
    filter(ISO3 == iso3) %>% 
    rename('Livehood_z' = 'Livelihood zones', 'NT_X'= "NT-X")
  
  to_do <- to_do %>% filter(ISO3 == iso3) %>%
      mutate(NWLD50 = NWLD, NWLD90 = NWLD, HSI_0 = HSI, HSI_1 = HSI, HSI_2 = HSI, HSI_3 = HSI, 
             THI_0 = THI, THI_1 = THI, THI_2 = THI, THI_3 = THI, SLGP_CV = SLGP ) %>% 
      dplyr::select(-THI, -HSI) 
    
  if(region == 'all'){
    var_s <- to_do %>% mutate( Regions = 'all', Livehood_z = 'all') %>% 
      mutate_at(.vars = vars(ATR:SLGP_CV ) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()}) %>% 
      group_by(ISO3, Country, Regions, Livehood_z) %>% 
      summarise_all(. , sum, na.rm = TRUE) %>% ungroup()
    
    title = 'Country'
  }else{
    var_s <- to_do %>% filter(Regions == region) %>%
      mutate_at(.vars = vars(ATR:SLGP_CV) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()})
  }
  
  var_to <- var_s %>% dplyr::select(-ISO3, -Country, -Regions, -Livehood_z) %>%
    tidyr::pivot_longer(cols = everything(), names_to = 'var', values_to = 'count') %>% 
    mutate(group = c('N/A', 'N/A', 'Drought', 'Drought', 'Drought', 'Drought', 'Drought', 'Agricultural', 'N/A', 'Waterlogging', 'Waterlogging', 'Waterlogging', 'Heat', 'Heat', 'Waterlogging', 'Waterlogging', 'N/A', 'N/A', 'Heat', 'Heat', 'N/A', 'N/A', 'Heat', 'Heat', 'Agricultural' )) %>% 
    filter(count > 0, group != 'N/A') 
  
  
  
  
  # Drought 
  if(var_to == 'NDWS'){}
  
  # Heat
  if(var_to == 'THI_2'){}
  
  # Waterlogging
  if(var_to == 'NWLD'){}
  
  
  
  for(s_i in season){
    # ppt <- readLines('D:/OneDrive - CGIAR/Desktop/P_indices_H/Profiles_AA/ppt_template_v2.Rmd')
    ppt <- readLines('D:/OneDrive - CGIAR/Desktop/WFP/WFP-profiles/ppt_template.Rmd')
    ppt <- ppt %>%
      purrr::map(.f = function(l){
        l <- gsub(pattern = 'Zone', replacement = region, x = l, fixed = T)
        l <- gsub(pattern = 'period', replacement = s_i, x = l, fixed = T)
        l <- gsub(pattern = 'COUNTRY', replacement = country, x = l, fixed = T)
        return(l)
      }) %>%
      unlist()
    writeLines(ppt, paste0(sld_dir,'/',country, '.Rmd'))
    # rmarkdown::render(ppt, paste0(sld_dir,'/',country, '.Rmd'))
  }
  
  return('Process done!\n')
}


create_slides(country = 'Tanzania', iso3 = 'CIV', countiesList = countiesList)
