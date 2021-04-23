# Create slides automatically
# A. Esquivel - H. Achicanoy - C.Saavedra 
# Alliance Bioversity-CIAT, 2020

options(warn = -1, scipen = 999)

suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse, rmarkdown))
library(dplyr)

sld_dir <- 'D:/OneDrive - CGIAR/Desktop/WFP/slides'
region <- 'all'
season <-   1:2 %>% paste0('s', .) %>% as.list()
iso3 <- 'BDI'
country <- 'Burundi'


create_slides <- function(country = 'Burundi', iso3 = iso3, season = season, region = region){

  to_do <- readxl::read_excel('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/regions_ind.xlsx') %>%
    filter(ISO3 == iso3) %>%
    rename('Livehood_z' = 'Livelihood zones', 'NT_X'= "NT-X") %>% 
    dplyr::select(-Short_Name)
# Por aqui incluir las variables 23. Aunque no estoy segura en donde. 
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

  for(s_i in season){
    ppt <- readLines('D:/OneDrive - CGIAR/Desktop/WFP/WFP-profiles/ppt_temp_F.Rmd')
    # ppt <- readLines('D:/OneDrive - CGIAR/Desktop/WFP/WFP-profiles/ppt_template.Rmd')
    ppt <- ppt %>%
      purrr::map(.f = function(l){
        l <- gsub(pattern = 'Zone', replacement = region, x = l, fixed = T)
        l <- gsub(pattern = 'period', replacement = s_i, x = l, fixed = T)
        l <- gsub(pattern = 'COUNTRY', replacement = country, x = l, fixed = T)
        # Drought
        if(sum(var_to$var == 'NDWS')==0){
          dr_v <- filter(var_to, group == 'Drought')[1,]$var
          l <- gsub(pattern = 'NDWS', replacement = dr_v, x = l, fixed = T)
        }
        # Heat
        if(sum(var_to == 'THI_2')==0){
          He_v <- filter(var_to, group == 'Heat')[1,]$var
          l <- gsub(pattern = 'THI_23', replacement = He_v, x = l, fixed = T)}
        # Waterlogging
        if(sum(var_to == 'NWLD')==0){
          Wa_v <- filter(var_to, group == 'Waterlogging')[1,]$var
        }
    
        return(l)}) %>%
      unlist()

    writeLines(ppt, paste0(sld_dir,'/',iso3, '_', region, '_', s_i, '.Rmd'))
    rmarkdown::render(paste0(sld_dir,'/',iso3, '_', region, '_', s_i, '.Rmd'))
  }
  
  return('Process done!\n')
}


create_slides(country = country, iso3 = iso3, season = season, region = region)
