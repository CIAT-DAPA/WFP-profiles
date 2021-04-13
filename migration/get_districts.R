options(warn = -1, scipen = 999)
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse, raster))

root <- 'D:/OneDrive - CGIAR/Migration'

reg <- readxl::read_excel(path = paste0(root,'/data/somalia_districts_control_table.xlsx'), sheet = 1)

districts <- reg$`HAROLD DISTRICT` %>% unique %>% na.omit %>% as.character
districts <- districts[-length(districts)]
shp <- raster::shapefile("//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/shps/somalia/som_gadm/Somalia_GADM2.shp")
for(district in districts){
  out <- paste0("//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/others/shps/somalia/",tolower(district),".shp")
  if(!file.exists(out)){
    shp2 <- shp[shp$NAME_2 == district,]
    plot(shp2)
    raster::shapefile(shp2, out)
  }
}

reg <- readxl::read_excel("D:/OneDrive - CGIAR/Migration/data/somalia_districts_control_table.xlsx", sheet = 1)
districts <- reg$`HAROLD DISTRICT` %>% unique %>% na.omit %>% as.character
districts <- districts[-length(districts)]
districts <- districts[1:30]
