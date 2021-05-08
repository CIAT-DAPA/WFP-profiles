# -------------------------------------------------- #
# Climate Risk Profiles -- CMIP6 data: GCM bias-correction performed by Quantile-mapping
# H. Achicanoy, A. Ghosh & C. Navarro
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# R options
options(warn = -1, scipen = 999)
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_main_functions.R') # Main functions

# Load libraries
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(qmap, future.apply, furrr, future, ncdf4, raster, tidyverse, compiler, vroom, gtools, fst))

OSys <- Sys.info()[1]
root <<- switch(OSys,
                'Linux'   = '/dapadfs/workspace_cluster_13/WFP_ClimateRiskPr',
                'Windows' = '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr')

# Quantile-mapping bias correction function for available pixels with solar radiation from NASA within a country
BC_Qmap_lnx <- function(his_obs = his_obs,
                    his_gcm = his_gcm,
                    fut_gcm = fut_gcm,
                    his_bc  = his_bc,
                    fut_bc  = fut_bc,
                    period  = period,
                    ncores  = 1)
{
  
  bc_qmap <<- function(df_obs, df_his_gcm, df_fut_gcm){
    cat('> Fitting the Qmap function per variable\n')
    prec_fit <- qmap::fitQmap(obs=df_obs$prec, mod=df_his_gcm$prec, method="RQUANT", qstep=0.01, wet.day=TRUE, na.rm=TRUE)
    tmax_fit <- qmap::fitQmap(obs=df_obs$tmax, mod=df_his_gcm$tmax, method="RQUANT", qstep=0.01, wet.day=FALSE, na.rm=TRUE)
    tmin_fit <- qmap::fitQmap(obs=df_obs$tmin, mod=df_his_gcm$tmin, method="RQUANT", qstep=0.01, wet.day=FALSE, na.rm=TRUE)
    
    cat('> Doing bias correction per variable historical GCMs\n')
    bc_his_gcm <- df_his_gcm
    bc_his_gcm$prec <- qmap::doQmap(x=df_his_gcm$prec, prec_fit, type="linear")
    bc_his_gcm$tmax <- qmap::doQmap(x=df_his_gcm$tmax, tmax_fit, type="linear")
    bc_his_gcm$tmin <- qmap::doQmap(x=df_his_gcm$tmin, tmin_fit, type="linear")
    
    cat('> Doing bias correction per variable future GCMs\n')
    bc_fut_gcm <- df_fut_gcm
    bc_fut_gcm$prec <- qmap::doQmap(x=df_fut_gcm$prec, prec_fit, type="linear")
    bc_fut_gcm$tmax <- qmap::doQmap(x=df_fut_gcm$tmax, tmax_fit, type="linear")
    bc_fut_gcm$tmin <- qmap::doQmap(x=df_fut_gcm$tmin, tmin_fit, type="linear")
    bc_data <- list(His = bc_his_gcm,
                    Fut = bc_fut_gcm)
    return(list(bc_data))
  }
  
  cat(paste0(' *** Performing Quantile-mapping bias correction***\n'))
  cat(paste0('>>> Loading obs data\n'))
  if(class(his_obs) == 'character'){
    his_obs <- his_obs %>%
      tidyft::parse_fst(path = .) %>%
      base::as.data.frame()
  } else {
    his_obs <- his_obs
  }
  his_obs$year <- NULL
  his_obs$id1 <- his_obs$id
  his_obs <- his_obs %>%
    tidyr::nest(Climate = c('id','date','prec','tmean','tmax','tmin','srad','wind','rh')) %>%
    dplyr::rename(id = 'id1') %>%
    dplyr::select(id, dplyr::everything(.))
  
  cat(paste0('>>> Impute missing data\n'))
  impute_missings <<- function(tbl = clim_data){
    Climate <- 1:nrow(tbl) %>%
      purrr::map(.f = function(i){
        df <- tbl$Climate[[i]]
        if(sum(is.na(df$tmean)) > 0){
          df$tmean[which(is.na(df$tmean))] <- median(df$tmean, na.rm = T)
        }
        if(sum(is.na(df$tmax)) > 0){
          df$tmax[which(is.na(df$tmax))] <- median(df$tmax, na.rm = T)
        }
        if(sum(is.na(df$tmin)) > 0){
          df$tmin[which(is.na(df$tmin))] <- median(df$tmin, na.rm = T)
        }
        if(sum(is.na(df$srad)) > 0){
          df$srad[which(is.na(df$srad))] <- median(df$srad, na.rm = T)
        }
        if(sum(is.na(df$prec)) > 0){
          df$prec[which(is.na(df$prec))] <- median(df$prec, na.rm = T)
        }
        if(sum(is.na(df$rh)) > 0){
          df$rh[which(is.na(df$rh))] <- median(df$rh, na.rm = T)
        }
        if(sum(is.na(df$wind)) > 0){
          df$wind[which(is.na(df$wind))] <- median(df$wind, na.rm = T)
        }
        return(df)
      })
    
    return(Climate)
  }
  his_obs$Climate <- impute_missings(tbl = his_obs)
  
  cat(paste0('>>> Loading historical GCM data\n'))
  if(class(his_gcm) == 'character'){
    his_gcm <- his_gcm %>%
      tidyft::parse_fst(path = .) %>%
      base::as.data.frame()
  } else {
    his_gcm <- his_gcm
  }
  his_gcm$id1 <- his_gcm$id
  his_gcm <- his_gcm %>%
    tidyr::nest(Climate = c('id','date','prec','tmax','tmin')) %>%
    dplyr::rename(id = 'id1') %>%
    dplyr::select(id, dplyr::everything(.))
  his_gcm$Climate <- impute_missings(tbl = his_gcm)
  
  cat(paste0('>>> Loading future GCM data\n'))
  if(class(fut_gcm) == 'character'){
    fut_gcm <- fut_gcm %>%
      tidyft::parse_fst(path = .) %>%
      base::as.data.frame()
  } else {
    fut_gcm <- fut_gcm
  }
  fut_gcm$id1 <- fut_gcm$id
  fut_gcm <- fut_gcm %>%
    tidyr::nest(Climate = c('id','date','prec','tmax','tmin')) %>%
    dplyr::rename(id = 'id1') %>%
    dplyr::select(id, dplyr::everything(.))
  fut_gcm$Climate <- impute_missings(tbl = fut_gcm)
  
  px <- intersect(his_obs$id, his_gcm$id)
  his_obs <- his_obs[his_obs$id %in% px,]
  his_gcm <- his_gcm[his_gcm$id %in% px,]
  fut_gcm <- fut_gcm[fut_gcm$id %in% px,]
  
  his_gcm_bc <<- his_gcm
  fut_gcm_bc <<- fut_gcm
  
  library(future.apply)
  plan(multiprocess, workers = ncores)
  bc_data <- future.apply::future_lapply(1:nrow(his_obs), FUN = function(i){
    tryCatch(expr={
      bc_data <<- bc_qmap(df_obs     = his_obs$Climate[[i]],
                          df_his_gcm = his_gcm$Climate[[i]],
                          df_fut_gcm = fut_gcm$Climate[[i]])
    },
    error = function(e){
      cat(paste0("Quantile mapping failed for pixel: ",i,"\n"))
      return("Done\n")
    })
    return(bc_data)
  }, future.seed = FALSE, USE.NAMES = FALSE)
  future:::ClusterRegistry("stop")
  gc(reset = T)
  
  his_gcm_bc$Climate <- bc_data %>% purrr::map(1) %>% purrr::map(1)
  fut_gcm_bc$Climate <- bc_data %>% purrr::map(1) %>% purrr::map(2)
  
  cat(paste0('Adding historical (1995-2014) srad, windspeed, and RH\n'))
  fut_gcm_bc$Climate <- 1:nrow(fut_gcm_bc) %>%
    purrr::map(.f = function(i){
      df <- fut_gcm_bc$Climate[[i]]
      his_obs_flt <- his_obs$Climate[[i]] %>%
        # dplyr::mutate(month = lubridate::month(date),
        #               day   = lubridate::day(date)) %>%
        # .[-which(.$month == 2 & .$day == 29),] %>%
        dplyr::filter(date >= as.Date('1995-01-01') & date <= as.Date('2014-12-31')) # Initial date: YYYY-01-02
      df$tmean <- (df$tmin + df$tmax)/2
      df$srad  <- his_obs_flt$srad
      df$wind  <- his_obs_flt$wind
      df$rh    <- his_obs_flt$rh
      return(df)
    })
  
  his_gcm_bc <- his_gcm_bc %>% tidyr::unnest(.)
  fut_gcm_bc <- fut_gcm_bc %>% tidyr::unnest(.)
  
  if(!file.exists(his_bc)){
    dir.create(dirname(his_bc),FALSE,TRUE)
    tidyft::export_fst(his_gcm_bc,his_bc)
  }
  dir.create(dirname(fut_bc),FALSE,TRUE)
  tidyft::export_fst(fut_gcm_bc,fut_bc)
  
  cat('Bias correction process completed successfully\n')
  
}

BC_Qmap_wnd <- function(his_obs = his_obs,
                        his_gcm = his_gcm,
                        fut_gcm = fut_gcm,
                        his_bc  = his_bc,
                        fut_bc  = fut_bc,
                        period  = period,
                        ncores  = 1)
{
  
  bc_qmap <<- function(df_obs, df_his_gcm, df_fut_gcm){
    cat('> Fitting the Qmap function per variable\n')
    prec_fit <- qmap::fitQmap(obs=df_obs$prec, mod=df_his_gcm$prec, method="RQUANT", qstep=0.01, wet.day=TRUE, na.rm=TRUE)
    tmax_fit <- qmap::fitQmap(obs=df_obs$tmax, mod=df_his_gcm$tmax, method="RQUANT", qstep=0.01, wet.day=FALSE, na.rm=TRUE)
    tmin_fit <- qmap::fitQmap(obs=df_obs$tmin, mod=df_his_gcm$tmin, method="RQUANT", qstep=0.01, wet.day=FALSE, na.rm=TRUE)
    
    cat('> Doing bias correction per variable historical GCMs\n')
    bc_his_gcm <- df_his_gcm
    bc_his_gcm$prec <- qmap::doQmap(x=df_his_gcm$prec, prec_fit, type="linear")
    bc_his_gcm$tmax <- qmap::doQmap(x=df_his_gcm$tmax, tmax_fit, type="linear")
    bc_his_gcm$tmin <- qmap::doQmap(x=df_his_gcm$tmin, tmin_fit, type="linear")
    
    cat('> Doing bias correction per variable future GCMs\n')
    bc_fut_gcm <- df_fut_gcm
    bc_fut_gcm$prec <- qmap::doQmap(x=df_fut_gcm$prec, prec_fit, type="linear")
    bc_fut_gcm$tmax <- qmap::doQmap(x=df_fut_gcm$tmax, tmax_fit, type="linear")
    bc_fut_gcm$tmin <- qmap::doQmap(x=df_fut_gcm$tmin, tmin_fit, type="linear")
    bc_data <- list(His = bc_his_gcm,
                    Fut = bc_fut_gcm)
    return(list(bc_data))
  }
  
  cat(paste0(' *** Performing Quantile-mapping bias correction***\n'))
  cat(paste0('>>> Loading obs data\n'))
  if(class(his_obs) == 'character'){
    his_obs <- his_obs %>%
      tidyft::parse_fst(path = .) %>%
      base::as.data.frame()
  } else {
    his_obs <- his_obs
  }
  his_obs$year <- NULL
  his_obs$id1 <- his_obs$id
  his_obs <- his_obs %>%
    tidyr::nest(Climate = c('id','date','prec','tmean','tmax','tmin','srad','wind','rh')) %>%
    dplyr::rename(id = 'id1') %>%
    dplyr::select(id, dplyr::everything(.))
  
  cat(paste0('>>> Impute missing data\n'))
  impute_missings <<- function(tbl = clim_data){
    Climate <- 1:nrow(tbl) %>%
      purrr::map(.f = function(i){
        df <- tbl$Climate[[i]]
        if(sum(is.na(df$tmean)) > 0){
          df$tmean[which(is.na(df$tmean))] <- median(df$tmean, na.rm = T)
        }
        if(sum(is.na(df$tmax)) > 0){
          df$tmax[which(is.na(df$tmax))] <- median(df$tmax, na.rm = T)
        }
        if(sum(is.na(df$tmin)) > 0){
          df$tmin[which(is.na(df$tmin))] <- median(df$tmin, na.rm = T)
        }
        if(sum(is.na(df$srad)) > 0){
          df$srad[which(is.na(df$srad))] <- median(df$srad, na.rm = T)
        }
        if(sum(is.na(df$prec)) > 0){
          df$prec[which(is.na(df$prec))] <- median(df$prec, na.rm = T)
        }
        if(sum(is.na(df$rh)) > 0){
          df$rh[which(is.na(df$rh))] <- median(df$rh, na.rm = T)
        }
        if(sum(is.na(df$wind)) > 0){
          df$wind[which(is.na(df$wind))] <- median(df$wind, na.rm = T)
        }
        return(df)
      })
    
    return(Climate)
  }
  his_obs$Climate <- impute_missings(tbl = his_obs)
  
  cat(paste0('>>> Loading historical GCM data\n'))
  if(class(his_gcm) == 'character'){
    his_gcm <- his_gcm %>%
      tidyft::parse_fst(path = .) %>%
      base::as.data.frame()
  } else {
    his_gcm <- his_gcm
  }
  his_gcm$id1 <- his_gcm$id
  his_gcm <- his_gcm %>%
    tidyr::nest(Climate = c('id','date','prec','tmax','tmin')) %>%
    dplyr::rename(id = 'id1') %>%
    dplyr::select(id, dplyr::everything(.))
  his_gcm$Climate <- impute_missings(tbl = his_gcm)
  
  cat(paste0('>>> Loading future GCM data\n'))
  if(class(fut_gcm) == 'character'){
    fut_gcm <- fut_gcm %>%
      tidyft::parse_fst(path = .) %>%
      base::as.data.frame()
  } else {
    fut_gcm <- fut_gcm
  }
  fut_gcm$id1 <- fut_gcm$id
  fut_gcm <- fut_gcm %>%
    tidyr::nest(Climate = c('id','date','prec','tmax','tmin')) %>%
    dplyr::rename(id = 'id1') %>%
    dplyr::select(id, dplyr::everything(.))
  fut_gcm$Climate <- impute_missings(tbl = fut_gcm)
  
  px <- intersect(his_obs$id, his_gcm$id)
  his_obs <- his_obs[his_obs$id %in% px,]
  his_gcm <- his_gcm[his_gcm$id %in% px,]
  fut_gcm <- fut_gcm[fut_gcm$id %in% px,]
  
  his_gcm_bc <<- his_gcm
  fut_gcm_bc <<- fut_gcm
  
  cl <- createCluster(ncores, export = list("root","his_obs","his_gcm","fut_gcm","bc_qmap","his_gcm_bc","fut_gcm_bc"), lib = list("tidyverse","raster","qmap"))
  
  bc_data <- 1:nrow(his_obs) %>% parallel::parLapply(cl, ., function(i){
    tryCatch(expr={
      bc_data <<- bc_qmap(df_obs     = his_obs$Climate[[i]],
                          df_his_gcm = his_gcm$Climate[[i]],
                          df_fut_gcm = fut_gcm$Climate[[i]])
    },
    error = function(e){
      cat(paste0("Quantile mapping failed for pixel: ",i,"\n"))
      return("Done\n")
    })
    return(bc_data)
  })
  parallel::stopCluster(cl)
  his_gcm_bc$Climate <- bc_data %>% purrr::map(1) %>% purrr::map(1)
  fut_gcm_bc$Climate <- bc_data %>% purrr::map(1) %>% purrr::map(2)
  
  cat(paste0('Adding historical (1995-2014) srad, windspeed, and RH\n'))
  fut_gcm_bc$Climate <- 1:nrow(fut_gcm_bc) %>%
    purrr::map(.f = function(i){
      df <- fut_gcm_bc$Climate[[i]]
      his_obs_flt <- his_obs$Climate[[i]] %>%
        # dplyr::mutate(month = lubridate::month(date),
        #               day   = lubridate::day(date)) %>%
        # .[-which(.$month == 2 & .$day == 29),] %>%
        dplyr::filter(date >= as.Date('1995-01-01') & date <= as.Date('2014-12-31')) # Initial date: YYYY-01-02
      df$tmean <- (df$tmin + df$tmax)/2
      df$srad  <- his_obs_flt$srad
      df$wind  <- his_obs_flt$wind
      df$rh    <- his_obs_flt$rh
      return(df)
    })
  
  his_gcm_bc <- his_gcm_bc %>% tidyr::unnest(.)
  fut_gcm_bc <- fut_gcm_bc %>% tidyr::unnest(.)
  
  if(!file.exists(his_bc)){
    dir.create(dirname(his_bc),FALSE,TRUE)
    tidyft::export_fst(his_gcm_bc,his_bc)
  }
  dir.create(dirname(fut_bc),FALSE,TRUE)
  tidyft::export_fst(fut_gcm_bc,fut_bc)
  
  cat('Bias correction process completed successfully\n')
  
}

# iso     <- 'TZA'
# model   <- 'INM-CM5-0'
# his_obs <- paste0(root,"/1.Data/observed_data/",iso,"/",iso,".fst")
# his_gcm <- paste0(root,"/1.Data/future_data/",model,"/",iso,"/downscale/1995-2014/",iso,".fst")
# his_bc  <- paste0(root,"/1.Data/future_data/",model,"/",iso,"/bias_corrected/1995-2014/",iso,".fst")
# c('2021-2040','2041-2060') %>%
#   purrr::map(.f = function(period){
#     fut_gcm <- paste0(root,"/1.Data/future_data/",model,"/",iso,"/downscale/",period,"/",iso,".fst")
#     fut_bc  <- paste0(root,"/1.Data/future_data/",model,"/",iso,"/bias_corrected/",period,"/",iso,".fst")
#     BC_Qmap_lnx(his_obs = his_obs,
#                 his_gcm = his_gcm,
#                 fut_gcm = fut_gcm,
#                 his_bc  = his_bc,
#                 fut_bc  = fut_bc,
#                 period  = period,
#                 ncores  = 1)
#   })
