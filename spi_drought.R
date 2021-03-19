options(warn = -1, scipen = 999)
pacman::p_load(tidyverse, tidyft, fst, raster, terra, sf)

root    <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'
country <- 'Haiti'
iso     <- 'HTI'

# Load municipalities/districts shapefile
shp <- raster::shapefile(paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_gadm/',country,'_GADM3.shp'))
# Identify administrative levels
adm <- grep('^NAME_', names(shp), value = T)
# Correct special characters
for(ad in adm){
  eval(parse(text=(paste0('shp$',ad,' <- iconv(x = shp$',ad,', from = "UTF-8", to = "latin1")'))))
}; rm(ad)
# Create a key
shp$key <- shp@data[,adm] %>%
  tidyr::unite(data = ., col = 'key', sep = '-') %>%
  dplyr::pull(key) %>%
  tolower(.)

# Read SPI time series results
spi <- paste0(root,'/7.Results/',country,'/past/',iso,'_spi.fst') %>%
  tidyft::parse_fst(path = .) %>%
  tidyft::select_fst()
spi <- fst::read_fst()
# Remove the first 4 months
spi <- spi %>% tidyr::drop_na()
# # Plot the time series
# spi$date <- as.Date(paste0(spi$month,'-',spi$year,'-01'))
# spi %>%
#   ggplot2::ggplot(aes(x = date, y = SPI, group = id)) +
#   ggplot2::geom_line(alpha = .1) +
#   ggplot2::theme_bw() +
#   ggplot2::geom_hline(yintercept = -1.5, colour = 'red')

# Load coords
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

# Remove areas without drought problems
r[r[] == 0] <- NA

'http://epsg.io/?q=Haiti'

rp <- raster::rasterToPolygons(x = r, dissolve = T)

int <- raster::intersect(x = rp, y = shp)
shp$area_tot <- raster::area(shp)/1e6
int$area <- raster::area(int)/1e6

tbl <- shp@data %>%
  dplyr::select(NAME_0, NAME_1, NAME_2, NAME_3, key, area_tot)
tbl2 <- dplyr::left_join(x = tbl, y = int@data %>% dplyr::select(key,area), by = 'key')
tbl2$area_perc <- tbl2$area/tbl2$area_tot
