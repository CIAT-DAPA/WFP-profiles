# =---
library(sf)
library(raster)
library(tidyverse)
# =---

root <- '//dapadfs.cgiarad.org/workspace_cluster_14/WFP_ClimateRiskPr'

## Defining country parameters
# Country
country <- 'Pakistan' # 'Tanzania'
iso3     <- 'PAK'     # 'TZA'


# =---a

location_map <- function(R_zone, iso3, country){
  # Reading the tables of future indices. 
  to_do <<- readxl::read_excel('//dapadfs/workspace_cluster_14/WFP_ClimateRiskPr/1.Data/regions_ind.xlsx') %>% 
    dplyr::filter(ISO3 == iso3) %>% 
    dplyr::rename('Livehood_z' = 'Livelihood zones', 'NT_X'= "NT-X")
  # =----------------------------------------------
  # Read all shp ... 
  # =----------------------------------------------
  
  # Read GDAM's shp at administrative level 1. 
  shp <<- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_gadm/",country,"_GADM1.shp"))
  shp_sf <-  shp %>%  sf::st_as_sf() %>% 
    dplyr::group_by(NAME_0) %>% dplyr::summarise()
  shp_sf <<- shp_sf
  
  # Regions shp
  regions_all <- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_regions/",tolower(iso3),"_regions.shp"))
  regions_all <- regions_all %>%  sf::st_as_sf() %>% 
    dplyr::group_by(region) %>% dplyr::summarise() %>% 
    dplyr::mutate(Short_Name = to_do$Short_Name)
  regions_all <<- regions_all
  
  
  # =--- World boundaries.
  map_world <- raster::shapefile(glue::glue('//dapadfs/workspace_cluster_14/WFP_ClimateRiskPr/1.Data/shps/all_country/all_countries.shp')) %>% 
    sf::st_as_sf()
  map_world <<- map_world
  
  ctn <- map_world$CONTINENT[which(map_world$ISO3 == iso3)]
  ctn <- map_world %>% dplyr::filter(CONTINENT == ctn)
  ctn <<- ctn
  # =--- 
  
  
  # =--- water sources. 
  glwd1 <- raster::shapefile('//dapadfs/workspace_cluster_14/WFP_ClimateRiskPr/1.Data/shps/GLWD/glwd_1.shp' ) 
  crs(glwd1) <- crs(shp)
  
  glwd2 <- raster::shapefile('//dapadfs/workspace_cluster_14/WFP_ClimateRiskPr/1.Data/shps/GLWD/glwd_2.shp' ) 
  crs(glwd2) <- crs(shp)
  
  if(!(iso3  %in% c('NPL', 'PAK', 'NER')) ){
    ext.sp <- raster::crop(glwd1, raster::extent(shp))
    glwd1 <-  rgeos::gSimplify(ext.sp, tol = 0.05, topologyPreserve = TRUE) %>%
      sf::st_as_sf()
    
    ext.sp2 <- raster::crop(glwd2, raster::extent(shp))
    glwd2 <- rgeos::gSimplify(ext.sp2, tol = 0.05, topologyPreserve = TRUE) %>%
      sf::st_as_sf()
  }else{   
    glwd1 <-  rgeos::gSimplify(glwd1, tol = 0.05, topologyPreserve = TRUE) %>%
      sf::st_as_sf()
    
    glwd2 <- rgeos::gSimplify(glwd2, tol = 0.05, topologyPreserve = TRUE) %>%
      sf::st_as_sf()
  }
  
  glwd1 <<-  glwd1
  glwd2 <<- glwd2
  # =--- 
  
  
  
  # =-----
  path <- glue::glue('//dapadfs/workspace_cluster_14/WFP_ClimateRiskPr/7.Results/{country}/results/')
  dir.create(path,recursive = TRUE)  
  
  
  if(R_zone == 'all'){
    zone <- regions_all # %>% sf::as_Spatial() 
    var_s <- to_do %>% dplyr::mutate( Regions = 'all', Livehood_z = 'all', Short_Name = 'all') %>% 
      dplyr::mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()}) %>% 
      dplyr::group_by(ISO3, Country, Regions, Livehood_z, Short_Name ) %>% 
      dplyr::summarise_all(. , sum, na.rm = TRUE) %>% dplyr::ungroup()
    
    title = 'Country'
  }else{
    zone <- dplyr::filter(regions_all, region == R_zone) # %>% sf::as_Spatial() 
    var_s <- to_do %>% dplyr::filter(Regions == R_zone) %>%
      dplyr::mutate_at(.vars = vars(ATR:SHI) , .funs = function(x){x <- ifelse(x == '-', 0, x) %>% as.integer()})
    
    title = dplyr::filter(to_do, Regions  == R_zone)$Short_Name   
  }
  # =---
  
  
  
  
  xlims <<- sf::st_bbox(shp_sf)[c(1, 3)]
  ylims <<- sf::st_bbox(shp_sf)[c(2, 4)]
  
  b <- ggplot() +
    geom_sf(data = ctn,  fill = '#AEB6BF', color = gray(.1)) +  # fill = '#AEB6BF' for different color to the continent and country
    geom_sf(data = shp_sf,  fill = '#D5DBDB', color = gray(.1)) +
    geom_sf(data = zone, aes(fill = Short_Name), color = gray(.1)) +
    geom_sf(data = glwd1, fill = 'lightblue', color = 'lightblue') +
    geom_sf(data = glwd2, fill = 'lightblue', color = 'lightblue') +
    coord_sf(xlim = xlims, ylim = ylims) +
    scale_fill_brewer(palette = "Set3") +
    labs(x = NULL, y = NULL, fill = NULL) +
    theme_bw() +
    theme(legend.position = 'bottom', 
          text = element_text(size=18), 
          axis.text        = element_blank(),
          legend.text = element_text(size=18),
          legend.title=element_text(size=18))  +  
    guides(fill = guide_legend(ncol = 1))
  
  ggsave(glue::glue('{path}/Location.png') , width = 8, height = 5.5, dpi = 300)
  
}

location_map(R_zone = 'all', iso3 = iso, country = country)