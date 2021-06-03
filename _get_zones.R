options(warn = -1, scipen = 999)
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse, raster))

country <- 'Niger'
iso     <- 'NER'

root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'

adm <- raster::shapefile(paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_gadm/',country,'_GADM2.shp'))
zns <- raster::shapefile(paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_regions/',tolower(iso),'_regions.shp'))

if(iso == 'NER'){zns <- spTransform(zns, raster::crs("+proj=longlat +datum=WGS84"))}

shp <- raster::intersect(adm, zns)
shp$key <- paste0('zone_',1:nrow(shp@data))

zones <- paste0('zone_',1:nrow(shp@data))

for(zone in zones){
  out <- paste0(root,"/1.Data/shps/",tolower(country),"/",tolower(iso),"_zones/",tolower(zone),".shp")
  if(!dir.exists(dirname(out))){dir.create(path = dirname(out),F,T)}
  if(!file.exists(out)){
    shp2 <- shp[shp$key == zone,]
    plot(shp2)
    raster::shapefile(shp2, out)
  }
}
