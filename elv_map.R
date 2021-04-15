# =----------------------
# Elevation Map
# A. Esquivel - H. Achicanoy - C.Saavedra 
# Alliance Bioversity-CIAT, 2021
# =----------------------

# R options
options(warn = -1, scipen = 999)

# Load libraries
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse, raster, tmap, fst, sf))

# Paths
OSys <- Sys.info()[1]
root <<- switch(OSys,
                'Linux'   = '/home/jovyan/work/cglabs',
                'Windows' = '//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr')

# Country...
alt <- raster::getData('alt', country = iso3, path = paste0(root,'/1.Data/shps/',country))

# Read GDAM's shp at administrative level 1. 
shp <<- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_gadm/",country,"_GADM1.shp"))
shp_sf <-  shp %>%  sf::st_as_sf() %>% 
  group_by(NAME_0) %>% summarise() %>% sf::as_Spatial()

shp_sf <<- shp_sf

# Regions shp
regions_all <- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_regions/",tolower(iso3),"_regions.shp"))
regions_all <- regions_all %>%  sf::st_as_sf() %>% 
  group_by(region) %>% summarise() %>% sf::as_Spatial()
regions_all <<- regions_all 

# =--- World boundaries.
map_world <- raster::shapefile(glue::glue('//dapadfs/workspace_cluster_8/climateriskprofiles/data/shps/all_country/all_countries.shp')) %>% 
  sf::st_as_sf()
map_world <<- map_world

ctn <- map_world$CONTINENT[which(map_world$ISO3 == iso3)]
ctn <- map_world %>% filter(CONTINENT == ctn)
ctn <<- ctn
# =--- 



# =--- water sources. 
glwd1 <- raster::shapefile('//dapadfs/workspace_cluster_8/climateriskprofiles/data/shps/GLWD/glwd_1.shp' ) 
crs(glwd1) <- crs(shp)
ext.sp <- raster::crop(glwd1, raster::extent(ctn))
glwd1 <-  rgeos::gSimplify(ext.sp, tol = 0.05, topologyPreserve = TRUE) %>%
  sf::st_as_sf()
glwd1 <<-  glwd1

glwd2 <- raster::shapefile('//dapadfs/workspace_cluster_8/climateriskprofiles/data/shps/GLWD/glwd_2.shp' ) 
crs(glwd2) <- crs(shp)
ext.sp2 <- raster::crop(glwd2, raster::extent(ctn))
glwd2 <- rgeos::gSimplify(ext.sp2, tol = 0.05, topologyPreserve = TRUE) %>%
  sf::st_as_sf()
glwd2 <<- glwd2
# =--- 


getAltitude <- function(iso3 = 'HTI', country = 'Haiti', Zone = 'all'){
  
  # =--------------------------------------------------
  if(Zone != 'all'){
    adm_c <- regions_all[regions_all$region == Zone,]
  }else{
    adm_c <- regions_all }
  
  alt_c <- alt %>% raster::crop(., adm_c) %>% raster::mask(., adm_c)
  
  alt_c <- alt_c %>% raster::rasterToPoints() %>% as_tibble() 
  xlims <- sf::st_bbox(adm_c)[c(1, 3)]
  ylims <- sf::st_bbox(adm_c)[c(2, 4)]
  
  test <- as_tibble(st_centroid(adm_c) %>% st_coordinates())  %>%
    mutate(name =  adm_c$region) 
  
  
  # =----------------------------
  
  pp <- ggplot() +
    geom_tile(data = alt_c %>% setNames(c('x', 'y', 'alt')), aes(x = x, y = y, fill =  alt)) +
    geom_sf(data = adm_c, fill = NA, color = gray(.2)) +
    geom_sf(data = glwd1, fill = 'lightblue', color = 'lightblue') +
    geom_sf(data = glwd2, fill = 'lightblue', color = 'lightblue') +
    coord_sf(xlim = round(xlims, 2), ylim = round(ylims, 2)) +
    scale_fill_gradientn(colours = terrain.colors(20),  
                         guide = guide_colourbar(barheight = 12 ,
                                                 barwidth = 2, label.theme = element_text(size = 35))) +
    scale_y_continuous(breaks = round(ylims, 2), n.breaks = 3) +
    scale_x_continuous(breaks = round(xlims, 2), n.breaks = 3) +
    labs(fill = glue::glue('Elevation (m)'), x = 'Longitude', y = 'Latitude') + # title = county ,
    ggrepel::geom_label_repel(data=test, aes(x=X, y=Y, label=name), 
                              # arrow = arrow(length = unit(0.03, "npc"), type = "closed", ends = "first"),
                              force = 10, 
                              size = 12) +
    theme_bw() + theme(text = element_text(size=35), 
                       legend.title=element_text(size=35), 
                       legend.spacing = unit(5, units = 'cm'),
                       legend.spacing.x = unit(1.0, 'cm'), plot.title = element_text(hjust = 0.5)) 
  
  
  path <- glue::glue('//dapadfs/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/{country}/results/maps/{R_zone}_{season}')
  dir.create(path,recursive = TRUE)  
  
  ggsave(glue::glue('{path}/Elevation.png'), width = 12, height = 12,  dpi = 300)
  
  return('Map saved successfully\n')}

# cnty_list <- c('Sikasso','Kayes','Segou','Mopti')
# for(i in 1:length(cnty_list))
# {getAltitude(iso3 = 'MLI', country = 'Mali', county = cnty_list[i])}


