# -------------------------------------------------- #
# Climate Risk Profiles -- Calc aridity index
# H. Achicanoy, J. Ramirez-Villegas
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# Input parameters:
#   clm: data frame with id, x, y, tmin, tmax, tmean, prec, and srad, columns
#   outfile: output file path
calc_tai <- function(clm = tbl, outfile = './TAI.fst'){
  
  if(!file.exists(outfile)){
    # Load packages
    if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
    suppressMessages(pacman::p_load(fst,envirem,gtools,tidyverse,raster))
    
    # Load CHIRPS template
    tmp <- raster::raster("//catalogue/BaseLineDataCluster01/observed/gridded_products/chirps/daily/chirps-v2.0.2020.01.01.tif")
    
    # Transform table to raster study area
    r <- raster::rasterFromXYZ(xyz = clm[,c('x','y')] %>% unique %>% dplyr::mutate(vals = 1),
                               res = raster::res(tmp),
                               crs = raster::crs(tmp))
    
    # ET SRAD
    srf <- list.dirs('//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/ET_SolRad', full.names = T, recursive = F)
    srf <- srf[-length(srf)]
    srf <- srf %>% gtools::mixedsort()
    srd <- srf %>% raster::stack()
    srd <- srd %>%
      raster::crop(., raster::extent(r)) %>%
      raster::resample(x = ., y = r)
    srd <- srd %>% raster::mask(., mask = r)
    names(srd) <- c(paste0('SRAD_0',1:9),paste0('SRAD_', 10:12))
    
    # Transform climate variables to the corresponding units
    clm$rnge  <- abs(clm$tmax - clm$tmin)
    
    # Calc monthly summaries per year
    summ <- clm %>%
      dplyr::mutate(Year  = lubridate::year(clm$date),
                    Month = lubridate::month(clm$date)) %>%
      dplyr::group_by(id, x, y, Year, Month) %>%
      dplyr::summarise(tmean = mean(tmean, na.rm = T),
                       rnge  = mean(rnge, na.rm = T),
                       prec  = sum(prec, na.rm = T))
    
    # Obtain yearly tables
    yr_summ <- summ %>%
      dplyr::group_by(Year) %>%
      dplyr::group_split(Year)
    
    # Assign precipitation names in envirem environment
    envirem::assignNames(solrad='SRAD_##', tmean = 'TMEAN_##', precip = 'PREC_##')
    
    # Calculate Aridity index over time
    TAI_over_time <- yr_summ %>%
      purrr::map(.f = function(df_yr){
        mnths <- df_yr %>%
          dplyr::group_by(Month) %>%
          dplyr::group_split(Month) %>%
          purrr::map(.f = function(db){
            vars <- c('tmean','prec','rnge')
            tst <- lapply(vars, function(v){
              r <- raster::rasterFromXYZ(xyz = db[,c('x','y',v)],
                                         res = raster::res(tmp),
                                         crs = raster::crs(tmp))
              return(r)
            })
            return(tst)
          })
        TMEAN <- mnths %>% purrr::map(1) %>% raster::stack()
        PREC  <- mnths %>% purrr::map(2) %>% raster::stack()
        TRNG  <- mnths %>% purrr::map(3) %>% raster::stack()
        
        names(TMEAN) <- c(paste0('TMEAN_0',1:9),paste0('TMEAN_', 10:12))
        names(PREC)  <- c(paste0('PREC_0',1:9),paste0('PREC_', 10:12))
        names(TRNG)  <- c(paste0('TRNG_0',1:9),paste0('TRNG_', 10:12))
        
        PET <- envirem::monthlyPET(TMEAN, srd, TRNG) %>% raster::stack()
        names(PET)  <- c(paste0('PET_0',1:9), paste0('PET_', 10:12))
        TAI <- envirem::aridityIndexThornthwaite(PREC, PET)
        return(TAI)
      })
    
    # Group results for all years in one stack
    TAI <- TAI_over_time %>% raster::stack()
    names(TAI) <- paste0('TAI_',1981:2019)
    df <- TAI %>% raster::rasterToPoints() %>% as.data.frame()
    df$id <- raster::cellFromXY(tmp, df[,c('x','y')])
    df <- df %>% dplyr::select(id,x,y,dplyr::everything(.))
    df <- df %>%
      tidyr::pivot_longer(cols = TAI_1981:TAI_2019, names_to = 'Year', values_to = 'TAI') %>%
      dplyr::mutate(Year = gsub('TAI_','',Year) %>% as.numeric)
    
    fst::write_fst(x = df, path = outfile)
  } else {
    cat('TAI index already calculated.\n')
  }
  return(cat('TAI index: calculated successfully!\n'))
}

# How to use it:
# root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'
# tbl <- fst::read_fst(paste0(root,'/1.Data/observed_data/HTI/HTI.fst'))
# tbl <- tbl %>%
#   tidyr::drop_na()
# calc_tai(clm = tbl)
