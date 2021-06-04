library(tidyverse)

# iso <- 'TZA'
# country <- 'Tanzania'
prj_structure <- function(iso, country){
  root <<- '/home/anighosh/data'
  main <<- paste0(root,'/WFP_ClimateRiskPr'); dir.create(main, showWarnings = F, recursive = T)
  hist <- paste0(main,'/1.Data/observed_data/',iso,'/year'); dir.create(hist, showWarnings = F, recursive = T); rm(hist)
  gcmL <- c('ACCESS-ESM1-5', 'EC-Earth3-Veg','INM-CM5-0','MPI-ESM1-2-HR','MRI-ESM2-0')
  gcmL %>%
    purrr::map(.f = function(gcm){
      futr <- paste0(main,'/1.Data/future_data/',gcm,'/',iso); dir.create(futr, showWarnings = F, recursive = T); rm(futr)
    })
  rm(gcmL)
  shps <- paste0(main,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_gadm'); dir.create(shps, showWarnings = F, recursive = T); rm(shps)
  shps <- paste0(main,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_regions'); dir.create(shps, showWarnings = F, recursive = T); rm(shps)
  shps <- paste0(main,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_wfp'); dir.create(shps, showWarnings = F, recursive = T); rm(shps)
  soil <- paste0(main,'/1.Data/soil/',iso); dir.create(soil, showWarnings = F, recursive = T); rm(soil)
  
  rslt <- paste0(main,'/7.Results/',country,'/past'); dir.create(rslt, showWarnings = F, recursive = T); rm(rslt)
  rslt <- paste0(main,'/7.Results/',country,'/future'); dir.create(rslt, showWarnings = F, recursive = T); rm(rslt)
  rslt <- paste0(main,'/7.Results/',country,'/results'); dir.create(rslt, showWarnings = F, recursive = T); rm(rslt)
  return(cat('Structure created!\n'))
}
iso <- c('TZA','SOM')
country <- c('Tanzania','Somalia')
purrr::map2(.x = iso, .y = country, .f = prj_structure)
