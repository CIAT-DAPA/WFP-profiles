# -------------------------------------------------- #
# Climate Risk Profiles -- SPI drought indicator calculation
# A. Esquivel, C. Saavedra, H. Achicanoy & J. Ramirez-Villegas
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# Input parameters:
#   spi_data: path of spi complete time series results
#   output: path of the spi drought indicator
#   iso: ISO 3 code in capital letters
#   country: country name first character in capital letter
#   seasons: corresponding seasons for each country
# Output:
#   data.frame with SPI drought values per season and year
calc_spi_drought <- function(spi_data = infile, output = outfile, country = country, iso = iso, seasons = seasons){
  
  if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
  pacman::p_load(tidyverse, tidyft, fst, raster, terra, sf)
  
  # root <<- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'
  OSys <<- Sys.info()[1]
  root <<- switch(OSys,
                  'Linux'   = '/dapadfs/workspace_cluster_14/WFP_ClimateRiskPr',
                  'Windows' = '//dapadfs.cgiarad.org/workspace_cluster_14/WFP_ClimateRiskPr')
  
  if(!file.exists(outfile)){
    # Load municipalities/districts shapefile
    # Identify the lowest admin level: to do
    lvl <- list.files(path = paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_gadm'), pattern = '.shp$', full.names = F, recursive = F)
    lvl <- regmatches(lvl, gregexpr("[[:digit:]]+", lvl)) %>% unlist() %>% as.numeric() %>% max(na.rm = T)
    shp <- raster::shapefile(paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_gadm/',country,'_GADM',lvl,'.shp'))
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
    spi <- infile %>%
      tidyft::parse_fst(path = .) %>%
      tidyft::select_fst(id,month,year,SPI) %>%
      base::as.data.frame()
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
    crd <- paste0(root,'/1.Data/observed_data/',iso,'/year/climate_1981_mod.fst') %>%
      tidyft::parse_fst(path = .) %>%
      tidyft::select_fst(id,x,y) %>%
      base::as.data.frame()
    crd <- unique(crd[,c('id','x','y')])
    spi <- dplyr::left_join(x = spi, y = crd, by = 'id')
    
    # Calculate area percent under drought conditions per season and year
    drgh <- 1:length(seasons) %>%
      purrr::map(.f = function(i){
        
        # Filter by each season
        season <- seasons[[i]]
        # Select the central month in the season
        mth    <- season[round(median(1:length(season)))]
        if(mth < 4){mth <- 4}
        
        spi_flt <- spi %>%
          dplyr::filter(year == mth) %>%
          dplyr::mutate(drought = ifelse(SPI < -1.5, 1, 0))
        
        drgh <- sort(unique(spi_flt$month)) %>%
          purrr::map(.f = function(yr){
            r <- spi_flt %>%
              dplyr::filter(month == yr) %>%
              dplyr::select(x, y, drought) %>%
              raster::rasterFromXYZ(xyz = ., res = 0.05, crs = raster::crs(shp))
            rv <- unique(r[])
            
            if(1 %in% rv){
              # Remove areas without drought problems
              r[r[] == 0] <- NA
              rp  <- raster::rasterToPolygons(x = r, dissolve = T)
              int <- raster::intersect(x = rp, y = shp)
              shp$area_tot <- raster::area(shp)/1e6
              
              if(!is.null(int)){
                int$area <- raster::area(int)/1e6
                res <- shp@data[,c(adm,'key','area_tot')]
                res <- dplyr::left_join(x = res, y = int@data[,c('key','area')], by = 'key')
                res$area_perc <- res$area/res$area_tot
                res$area_perc[is.na(res$area_perc)] <- 0
              } else {
                res <- shp@data[,c(adm,'key','area_tot')]
                res$area <- 0
                res$area_perc <- res$area/res$area_tot
              }
              
            } else {
              shp$area_tot <- raster::area(shp)/1e6
              res <- shp@data[,c(adm,'key','area_tot')]
              res$area <- 0
              res$area_perc <- res$area/res$area_tot
            }
            
            res <- res[,c(adm,'key','area_perc')]
            names(res)[ncol(res)] <- paste0('Y',yr)
            return(res)
          }) %>%
          purrr::reduce(dplyr::left_join, by = c(adm,'key'))
        
        tmp <- shp
        tmp@data <- dplyr::left_join(x = tmp@data %>% dplyr::select(adm,'key'),
                                     y = drgh,
                                     by = c(adm,'key'))
        
        drgh_px <- cbind(crd,raster::extract(x = tmp, y = crd[,c('x','y')])) %>% tidyr::drop_na()
        drgh_px <- drgh_px %>% dplyr::select(id,x,y,grep(pattern = '^Y', x = names(drgh_px)))
        drgh_px <- drgh_px %>% tidyr::pivot_longer(cols = grep(pattern = '^Y', x = names(drgh_px)), names_to = 'year', values_to = 'spi')
        drgh_px$year <- gsub('Y','',drgh_px$year)
        drgh_px$season <- paste0('s',i)
        return(drgh_px)
      }) %>%
      dplyr::bind_rows()
    fst::write_fst(drgh, outfile)
    cat('SPI drought indicator successfully calculated\n')
  } else {
    cat('SPI drought indicator is already calculated\n')
  }
  
}
