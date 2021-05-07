# -------------------------------------------------- #
# Climate Risk Profiles -- Filter climate to regions
# H. Achicanoy & A. Esquivel
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# Input parameters:
#   iso: ISO 3 code in capital letters
#   country: country name first character in capital letter
# Output:
#   Data frame with climate filtered for the areas of interest
# 1. Migration analysis
flt_clm_subunits <- function(iso = 'SOM', country = 'Somalia', district = 'Caabudwaaq'){
  
  # Load packages
  if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
  suppressMessages(pacman::p_load(tidyverse,raster,terra,tidyft,sf))
  
  root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'
  
  cat('>>> Load all coords\n')
  crd <- paste0(root,'/1.Data/observed_data/',iso,'/year/climate_1981_mod.fst')
  crd <- crd %>%
    tidyft::parse_fst(path = .) %>%
    tidyft::select_fst(id, x, y) %>%
    tidyft::distinct_dt() %>%
    base::as.data.frame()
  
  cat('>>> Regions shape and add a buffer of 5 km\n')
  shp <- sf::read_sf(paste0(root,'/1.Data/others/shps/',tolower(country),'/',tolower(district),'.shp'))
  shp <- sf::st_buffer(shp, dist = 0.05) %>% sf::st_union(.) %>% sf::as_Spatial() %>% terra::vect()
  ref <- terra::rast(paste0(root,'/1.Data/chirps-v2.0.2020.01.01.tif'))
  ref <- terra::crop(ref, terra::ext(shp))
  rst <- terra::rasterize(x = shp, y = ref)
  
  cat('>>> Filter coords in regions of interest\n')
  crd$sl <- terra::extract(x = rst, y = crd[,c('x','y')]) %>% unlist() %>% as.numeric()
  crd <- crd[complete.cases(crd),]
  pft <<- crd$id
  
  cat('>>> Filter climate table to filtered coords\n')
  clm <- paste0(root,'/1.Data/observed_data/',iso,'/',iso,'.fst')
  clm <- clm %>%
    tidyft::parse_fst(path = .) %>%
    tidyft::filter_fst(id %in% pft) %>%
    base::as.data.frame()
  
  return(clm)
  
}
# 2. Historic climate data
flt_clm_subunits2 <- function(iso = 'SOM', country = 'Somalia', district = 'Caabudwaaq'){
  
  # Load packages
  if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
  suppressMessages(pacman::p_load(tidyverse,raster,terra,tidyft,sf))
  
  root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'
  
  cat('>>> Load all coords\n')
  crd <- paste0(root,'/1.Data/observed_data/',iso,'/year/climate_1981_mod.fst')
  crd <- crd %>%
    tidyft::parse_fst(path = .) %>%
    tidyft::select_fst(id, x, y) %>%
    tidyft::distinct_dt() %>%
    base::as.data.frame()
  
  cat('>>> Regions shape and add a buffer of 5 km\n')
  shp <- sf::read_sf(paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_zones/',tolower(district),'.shp'))
  shp <- sf::st_buffer(shp, dist = 0.05) %>% sf::st_union(.) %>% sf::as_Spatial() %>% terra::vect()
  ref <- terra::rast(paste0(root,'/1.Data/chirps-v2.0.2020.01.01.tif'))
  ref <- terra::crop(ref, terra::ext(shp))
  rst <- terra::rasterize(x = shp, y = ref)
  
  cat('>>> Filter coords in regions of interest\n')
  crd$sl <- terra::extract(x = rst, y = crd[,c('x','y')]) %>% unlist() %>% as.numeric()
  crd <- crd[complete.cases(crd),]
  pft <<- crd$id
  
  cat('>>> Filter climate table to filtered coords\n')
  clm <- paste0(root,'/1.Data/observed_data/',iso,'/',iso,'.fst')
  clm <- clm %>%
    tidyft::parse_fst(path = .) %>%
    tidyft::filter_fst(id %in% pft) %>%
    base::as.data.frame()
  
  return(clm)
  
}
# 3. Future climate data
flt_clm_subunits3 <- function(iso = 'SOM', country = 'Somalia', district = 'Caabudwaaq', model, period){
  
  # Load packages
  if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
  suppressMessages(pacman::p_load(tidyverse,raster,terra,tidyft,sf))
  
  root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'
  
  cat('>>> Load all coords\n')
  crd <- paste0(root,'/1.Data/observed_data/',iso,'/year/climate_1981_mod.fst')
  crd <- crd %>%
    tidyft::parse_fst(path = .) %>%
    tidyft::select_fst(id, x, y) %>%
    tidyft::distinct_dt() %>%
    base::as.data.frame()
  
  cat('>>> Regions shape and add a buffer of 5 km\n')
  shp <- sf::read_sf(paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_zones/',tolower(district),'.shp'))
  shp <- sf::st_buffer(shp, dist = 0.05) %>% sf::st_union(.) %>% sf::as_Spatial() %>% terra::vect()
  ref <- terra::rast(paste0(root,'/1.Data/chirps-v2.0.2020.01.01.tif'))
  ref <- terra::crop(ref, terra::ext(shp))
  rst <- terra::rasterize(x = shp, y = ref)
  
  cat('>>> Filter coords in regions of interest\n')
  crd$sl <- terra::extract(x = rst, y = crd[,c('x','y')]) %>% unlist() %>% as.numeric()
  crd <- crd[complete.cases(crd),]
  pft <<- crd$id
  
  cat('>>> Filter climate table to filtered coords\n')
  clm <- paste0(root,'/1.Data/future_data/',model,'/',iso,'/bias_corrected/',period,'/',iso,'.fst')
  clm <- clm %>%
    tidyft::parse_fst(path = .) %>%
    tidyft::filter_fst(id %in% pft) %>%
    base::as.data.frame()
  
  return(clm)
  
}