Elv_map <- function(iso3, country){
  
  # Country...
  if(country == 'Guinee'){
    alt <- raster::raster('{root}/1.Data/shps/guinee/GIN_alt/GIN_alt.gri')
  }else{
    alt <- raster::getData('alt', country = iso3, path = paste0(root,'/1.Data/shps/',country))
  }
  
  ext.files <- ls() 
  
  
  # Read GDAM's shp at administrative level 1. 
  shp <<- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_gadm/",country,"_GADM1.shp"))
  shp_sf <-  shp %>%  sf::st_as_sf() %>% 
    dplyr::group_by(NAME_0) %>% dplyr::summarise() %>% sf::st_as_sf(shp_sf)  
  
  shp_sf <<- shp_sf
  
  if(sum(ext.files %in% c('regions_all')) == 0){
    # Regions shp
    regions_all <- raster::shapefile(paste0(root , "/1.Data/shps/", tolower(country), "/",tolower(iso3),"_regions/",tolower(iso3),"_regions.shp"))
    regions_all <- regions_all %>%  sf::st_as_sf() %>% 
      dplyr::group_by(region) %>% dplyr::summarise() %>% sf::as_Spatial()
    regions_all <<- regions_all 
  }
  
  if(sum(ext.files %in% c('glwd1', 'glwd2')) < 2){
    # =--- water sources. 
    glwd1 <- raster::shapefile(glue::glue('{root}/1.Data/shps/GLWD/glwd_1_fixed.shp' ) )
    crs(glwd1) <- crs(shp)
    
    glwd2 <- raster::shapefile(glue::glue('{root}/1.Data/shps/GLWD/glwd_2_fixed.shp' ) )
    crs(glwd2) <- crs(shp)
    
    # c('NPL', 'PAK', 'NER')
    ext.sp <- raster::crop(glwd1, raster::extent(shp))
    glwd1 <-  rgeos::gSimplify(ext.sp, tol = 0.05, topologyPreserve = TRUE) %>%
      sf::st_as_sf()
    
    ext.sp2 <- raster::crop(glwd2, raster::extent(shp))
    glwd2 <- rgeos::gSimplify(ext.sp2, tol = 0.05, topologyPreserve = TRUE) %>%
      sf::st_as_sf()
    
    glwd1 <<-  glwd1; glwd2 <<- glwd2
  }
  
  # =----
  getAltitude <- function(iso3 = 'HTI', country = 'Haiti', Zone = 'all'){
    
    adm_c <- regions_all
    # adm_c <- regions_all[regions_all$region == 'region_4', ] 
    alt_c <- alt %>% raster::crop(., adm_c) %>% raster::mask(., adm_c)
    
    alt_c <- alt_c %>% raster::rasterToPoints() %>% as_tibble() 
    xlims <- sf::st_bbox(adm_c)[c(1, 3)]
    ylims <- sf::st_bbox(adm_c)[c(2, 4)]
    
    adm_c <- sf::st_as_sf(adm_c)   
    test <- tibble::as_tibble(st_centroid(adm_c) %>% st_coordinates())  %>%
      dplyr::mutate(name =  adm_c$region) 
    
    # =----------------------------
    
    pp <- ggplot() +
      geom_tile(data = alt_c %>% setNames(c('x', 'y', 'alt')), aes(x = x, y = y, fill =  alt)) +
      geom_sf(data = shp_sf, fill = NA, color = gray(.5)) +
      geom_sf(data = adm_c, fill = NA, color = gray(.2)) +
      geom_sf(data = glwd1, fill = 'lightblue', color = 'lightblue') +
      geom_sf(data = glwd2, fill = 'lightblue', color = 'lightblue') +
      coord_sf(xlim = round(xlims, 2), ylim = round(ylims, 2)) +
      scale_fill_gradientn(colours = terrain.colors(20),  
                           guide = guide_colourbar(barheight = 12 ,
                                                   barwidth = 2, label.theme = element_text(size = 35))) +
      scale_y_continuous(breaks = round(ylims, 2), n.breaks = 3) +
      scale_x_continuous(breaks = round(xlims, 2), n.breaks = 3) +
      labs(fill = glue::glue('Elevation (m)'), x = NULL, y = NULL) + # title = county ,
      theme_bw() + theme(text = element_text(size=35), 
                         legend.title=element_text(size=35), 
                         legend.spacing = unit(5, units = 'cm'),
                         legend.spacing.x = unit(1.0, 'cm'), plot.title = element_text(hjust = 0.5)) 
    
    
    path <- glue::glue('{root}/7.Results/{country}/results/')
    dir.create(path,recursive = TRUE)  
    
    ggsave(glue::glue('{path}/Elevation.png'), width = 12, height = 12,  dpi = 300)
    
    return('Map saved successfully\n')}
  
  getAltitude(iso3 = iso3, country = country, Zone = 'all')
}