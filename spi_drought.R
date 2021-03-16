options(warn = -1, scipen = 999)
pacman::p_load(tidyverse, fst, raster, terra, sf)

shp <- raster::shapefile("//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/shps/haiti/hti_gadm/Haiti_GADM3.shp")

spi <- fst::read_fst("//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/Haiti/past/HTI_spi.fst")
spi <- spi %>% tidyr::drop_na()
spi$date <- as.Date(paste0(spi$month,'-',spi$year,'-01'))
spi %>%
  ggplot2::ggplot(aes(x = date, y = SPI, group = id)) +
  ggplot2::geom_line(alpha = .1) +
  ggplot2::theme_bw() +
  ggplot2::geom_hline(yintercept = -1.5, colour = 'red')

crd <- fst::read_fst("//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/observed_data/HTI/year/climate_1981_mod.fst")
crd <- unique(crd[,c('id','x','y')])
spi <- dplyr::left_join(x = spi, y = crd, by = 'id')
spi_flt <- spi %>%
  dplyr::filter(year == 9) %>%
  dplyr::mutate(drought = ifelse(SPI < -1.5, 1, 0))

r <- spi_flt %>%
  dplyr::filter(month == 2019) %>%
  dplyr::select(x, y, drought) %>%
  raster::rasterFromXYZ(xyz = ., res = 0.05, crs = raster::crs(shp))

plot(r)
plot(shp, add = T)

shp$area_sqkm <- raster::area(shp)/1000000

shp$AFFECTED_AREA <- shp$key %>%
  purrr::map(.f = function(district){
    dst <- shp[shp$key == district,]
    clp <- raster::intersect(dst, buffers2)
    area_perc <- round(raster::area(clp)/10000000, 1)/round(raster::area(dst)/10000000, 1)
    return(area_perc)
  }) %>%
  unlist()
