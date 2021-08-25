# -------------------------------------------------- #
# Climate Risk Profiles -- Main functions
# A. Esquivel, C. Saavedra, H. Achicanoy & J. Ramirez-Villegas
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# Windows parallelization functions
clusterExport <- local({
  gets <- function(n, v) { assign(n, v, envir = .GlobalEnv); NULL }
  function(cl, list, envir = .GlobalEnv) {
    ## do this with only one clusterCall--loop on slaves?
    for (name in list) {
      clusterCall(cl, gets, name, get(name, envir = envir))
    }
  }
})
createCluster <- function(noCores, logfile = "/dev/null", export = NULL, lib = NULL) {
  require(doSNOW)
  cl <- makeCluster(noCores, type = "SOCK", outfile = logfile)
  if(!is.null(export)) clusterExport(cl, export)
  if(!is.null(lib)) {
    plyr::l_ply(lib, function(dum) { 
      clusterExport(cl, "dum", envir = environment())
      clusterEvalQ(cl, library(dum, character.only = TRUE))
    })
  }
  registerDoSNOW(cl)
  return(cl)
}

# Generate chunks
chunk <- function(vect, size){
  split(vect, ceiling(seq_along(vect)/size))
}

# Agro-climatic indices
rsum.lapply <- function(x, n=3L) # Calculate rollin sum
{
  lapply(1:(length(x)-n+1), function(i)
  {
    # Sum for n consecutive days
    z <- sum(x[i:(i+n-1)])
    # Indices used to calculate the sum
    seq.sum <- as.numeric(i:(i+n-1))
    # List with SUM and INDICES
    results <- list(z, seq.sum)
    return(results)
  })
}
cumulative.r.sum <- function(results){ unlist(lapply(results, function(x){z <- x[[1]]; return(z)})) } # Extract the SUM
is.leapyear <- function(year){ return(((year %% 4 == 0) & (year %% 100 != 0)) | (year %% 400 == 0)) } # Function to identify leap years

## NDD. Average number of dry days
calc_ndd <- function(PREC, p_thresh = 1){
  dry_days <- sum(PREC < p_thresh, na.rm = T)
  return(dry_days)
}
calc_nddCMP <- compiler::cmpfun(calc_ndd)

## CDD. Maximum number of consecutive dry days
calc_cdd <- function(PREC, p_thresh=1){
  runs <- rle(PREC < p_thresh)
  cons_days <- max(runs$lengths[runs$values==1], na.rm=TRUE)
  return(cons_days)
}
calc_cddCMP <- compiler::cmpfun(calc_cdd)

## CDD. Maximum number of consecutive days with TMAX above t_thresh
calc_cdd_temp <- function(TMAX, t_thresh=37){
  runs <- rle(TMAX > t_thresh)
  cons_days <- max(runs$lengths[runs$values==1], na.rm=TRUE)
  return(cons_days)
}
calc_cdd_tempCMP <- compiler::cmpfun(calc_cdd_temp)

## P5D. Maximum 5-day running average precipitation
calc_p5d <- function(PREC){
  runAvg <- caTools::runmean(PREC, k=5, endrule='NA')
  runAvg <- max(runAvg, na.rm=TRUE)
  return(runAvg)
}
calc_p5dCMP <- compiler::cmpfun(calc_p5d)

## NT35. Number of days with max. temperature above 35?C
calc_hts <- function(tmax, t_thresh=35) {
  hts <- length(which(tmax >= t_thresh))
  return(hts)
}
calc_htsCMP <- compiler::cmpfun(calc_hts)

## P95. 95th percentile of daily precipitation
calc_p95 <- function(PREC){
  quantile(PREC, probs = .95, na.rm = T)
}
calc_p95CMP <- compiler::cmpfun(calc_p95)

### Mean temperature ***
tmean <- function(tmax, tmin, season_ini=1, season_end=365){
  tavg <- lapply(1:nrow(tmax), function(i){
    tavg <- (tmax[i, season_ini:season_end]+tmin[i, season_ini:season_end])/2
  })
  tavg <- do.call(rbind, tavg)
  return(tavg)
}
tmeanCMP <- compiler::cmpfun(tmean)

### Total prec at year ***
calc_totprec <- function(prec){
  totprec <- sum(prec, na.rm=T)
  return(totprec)
}
calc_totprecCMP <- compiler::cmpfun(calc_totprec)

### Maximum number of consecutive dry days, prec < 1 mm
dr_stress <- function(PREC, p_thresh=1){
  runs <- rle(PREC < p_thresh)
  cons_days <- max(runs$lengths[runs$values==1], na.rm=TRUE)
  return(cons_days)
}
dr_stressCMP <- compiler::cmpfun(dr_stress)

### number of prec days
calc_precdays <- function(x, season_ini=1, season_end=365, p_thresh=0.1) {
  precdays <- length(which(x$prec[season_ini:season_end] > p_thresh))
  return(precdays)
}

### maximum consecutive dry days
calc_max_cdd <- function(x, year=2000, season_ini=1, season_end=365, p_thresh=0.1) {
  cdd <- 0; cdd_seq <- c()
  for (i_x in season_ini:season_end) {
    if (x$prec[i_x] < p_thresh) {
      cdd <- cdd+1
    } else {
      cdd_seq <- c(cdd_seq, cdd)
      cdd <- 0
    }
  }
  max_cdd <- max(cdd_seq)
  return(max_cdd)
}

### mean consecutive dry days
calc_mean_cdd <- function(x, season_ini=1, season_end=365, p_thresh=0.1) {
  cdd <- 0; cdd_seq <- c()
  for (i_x in season_ini:season_end) {
    if (x$prec[i_x] < p_thresh) {
      cdd <- cdd+1
    } else {
      cdd_seq <- c(cdd_seq, cdd)
      cdd <- 0
    }
  }
  mean_cdd <- mean(cdd_seq[which(cdd_seq > 0)],na.rm=T)
  return(mean_cdd)
}

### number of prec days
calc_txxdays <- function(x, season_ini=1, season_end=365, t_thresh=30) {
  x$TDAY <- x$tmax*0.75 + x$tmin*0.25 #day temperature
  txxdays <- length(which(x$TDAY[season_ini:season_end] > t_thresh))
  return(txxdays)
}

### number of prec days
calc_tnndays <- function(x, season_ini=1, season_end=365, t_thresh=10) {
  x$TDAY <- x$tmax*0.75 + x$tmin*0.25 #day temperature
  tnndays <- length(which(x$TDAY[season_ini:season_end] < t_thresh))
  return(tnndays)
}

# ### calculate soilcap in mm (old version)
# soilcap_calc <- function(x, minval, maxval) {
#   rdepth <- max(c(x[4],minval)) #cross check
#   rdepth <- min(c(rdepth,maxval)) #cross-check
#   wc_df <- data.frame(depth=c(2.5,10,22.5,45,80,150),wc=(x[5:10])*.01)
#   if (!rdepth %in% wc_df$depth) {
#     wc_df1 <- wc_df[which(wc_df$depth < rdepth),]
#     wc_df2 <- wc_df[which(wc_df$depth > rdepth),]
#     y1 <- wc_df1$wc[nrow(wc_df1)]; y2 <- wc_df2$wc[1]
#     x1 <- wc_df1$depth[nrow(wc_df1)]; x2 <- wc_df2$depth[1]
#     ya <- (rdepth-x1) / (x2-x1) * (y2-y1) + y1
#     wc_df <- rbind(wc_df1,data.frame(depth=rdepth,wc=ya),wc_df2)
#   }
#   wc_df <- wc_df[which(wc_df$depth <= rdepth),]
#   wc_df$soilthick <- wc_df$depth - c(0,wc_df$depth[1:(nrow(wc_df)-1)])
#   wc_df$soilcap <- wc_df$soilthick * wc_df$wc
#   soilcp <- sum(wc_df$soilcap) * 10 #in mm
#   return(soilcp)
# }
soilcap_calc <- function(x, y, rdepth=60, minval, maxval) {
  if (length(x) != length(y)) {stop("length of x and y must be the same")}
  rdepth <- max(c(rdepth,minval)) #cross check
  rdepth <- min(c(rdepth,maxval)) #cross-check
  wc_df <- data.frame(depth=y,wc=x)
  if (!rdepth %in% wc_df$depth) {
    wc_df1 <- wc_df[which(wc_df$depth < rdepth),]
    wc_df2 <- wc_df[which(wc_df$depth > rdepth),]
    y1 <- wc_df1$wc[nrow(wc_df1)]; y2 <- wc_df2$wc[1]
    x1 <- wc_df1$depth[nrow(wc_df1)]; x2 <- wc_df2$depth[1]
    ya <- (rdepth-x1) / (x2-x1) * (y2-y1) + y1
    wc_df <- rbind(wc_df1,data.frame(depth=rdepth,wc=ya),wc_df2)
  }
  wc_df <- wc_df[which(wc_df$depth <= rdepth),]
  wc_df$soilthick <- wc_df$depth - c(0,wc_df$depth[1:(nrow(wc_df)-1)])
  wc_df$soilcap <- wc_df$soilthick * wc_df$wc
  soilcp <- sum(wc_df$soilcap) * 10 #in mm
  return(soilcp)
}

# potential evapotranspiration
peest <- function(srad,tmin,tmax) {
  #constants
  albedo <- 0.2
  vpd_cte <- 0.7
  
  #soil heat flux parameters
  a_eslope=611.2
  b_eslope=17.67
  c_eslope=243.5
  
  #input parameters
  tmean <- (tmin+tmax)/2
  
  #net radiation
  rn = (1-albedo) * srad
  
  #soil heat flux
  eslope=a_eslope*b_eslope*c_eslope/(tmean+c_eslope)^2*exp(b_eslope*tmean/(tmean+c_eslope))
  
  #estimate vpd
  esat_min=0.61120*exp((17.67*tmin)/(tmin+243.5))
  esat_max=0.61120*exp((17.67*tmax)/(tmax+243.5))
  vpd=vpd_cte*(esat_max-esat_min) #kPa
  
  #Priestley-Taylor
  pt_const=1.26
  pt_fact=1
  vpd_ref=1
  psycho=62
  rho_w=997
  rlat_ht=2.26E6
  
  pt_coef=pt_fact*pt_const
  pt_coef = 1 + (pt_coef-1) * vpd / vpd_ref
  
  #*10^6? To convert fluxes MJ to J
  #rlat_ht? Latent heat flux to water flux
  #100/rho_w? Kg/m^2 to cm
  et_max=(pt_coef * rn * eslope/(eslope+psycho) * 10^6 / rlat_ht * 100/rho_w)*10 #in mm
  return(et_max)
}

#the two functions below estimate the ea/ep
#based on Jones (1987)
#ea/ep: actual to potential evapotranspiration ratio
eabyep_calc <- function(soilcp=100,soilsat=100,cropfc=1,avail=50,rain,evap) {
  avail <- min(c(avail,soilcp))
  eratio <- eabyep(soilcp,avail)
  demand <- eratio*cropfc*evap
  result <- avail + rain - demand
  logging <- result - soilcp
  logging <- max(c(logging,0))
  logging <- min(c(soilsat,logging))
  runoff <- result - (logging+soilcp)
  avail <- min(c(soilcp,result))
  avail <- max(c(avail,0))
  runoff <- max(c(runoff,0))
  out <- data.frame(AVAIL=avail,DEMAND=demand,ERATIO=eratio,RAIN=rain,LOGGING=logging,RUNOFF=runoff)
  return(out)
}

#ea/ep function
eabyep <- function(soilcp,avail) {
  percwt <- min(c(100,avail/soilcp*100))
  percwt <- max(c(1,percwt))
  eratio <- min(c(percwt/(97-3.868*sqrt(soilcp)),1))
  return(eratio)
}

# # wrapper to calculate the water balance modeling variables (old)
# watbal_wrapper <- function(out_all, soilcp){
#   out_all$Etmax <- out_all$AVAIL <- out_all$ERATIO <- out_all$RUNOFF <- out_all$DEMAND <- out_all$CUM_prec <- NA
#   for (d in 1:nrow(out_all)) {
#     out_all$Etmax[d] <- peest(out_all$srad[d], out_all$tmin[d], out_all$tmax[d])
#     
#     if (d==1) {
#       out_all$CUM_prec[d] <- out_all$prec[d]
#       sfact <- eabyep_calc(soilcp=soilcp, cropfc=1, avail=0, prec=out_all$prec[d], evap=out_all$Etmax[d])
#       out_all$AVAIL[d] <- sfact$AVAIL
#       out_all$ERATIO[d] <- sfact$ERATIO
#       out_all$RUNOFF[d] <- sfact$RUNOFF
#       out_all$DEMAND[d] <- sfact$DEMAND
#       
#     } else {
#       out_all$CUM_prec[d] <- out_all$CUM_prec[d-1] + out_all$prec[d]
#       sfact <- eabyep_calc(soilcp=soilcp, cropfc=1, avail=out_all$AVAIL[d-1], prec=out_all$prec[d], evap=out_all$Etmax[d])
#       out_all$AVAIL[d] <- sfact$AVAIL
#       out_all$ERATIO[d] <- sfact$ERATIO
#       out_all$RUNOFF[d] <- sfact$RUNOFF
#       out_all$DEMAND[d] <- sfact$DEMAND
#     }
#   }
#   return(out_all)
# }
#wrapper to calculate the water balance modeling variables
watbal_wrapper <- function(out_all, soilcp, soilsat)  {
  out_all$ETMAX <- out_all$AVAIL <- out_all$ERATIO <- out_all$LOGGING <- out_all$RUNOFF <- out_all$DEMAND <- out_all$CUM_RAIN <- NA
  for (d in 1:nrow(out_all)) {
    out_all$ETMAX[d] <- peest(out_all$srad[d],out_all$tmin[d],out_all$tmax[d])
    
    if (d==1) {
      out_all$CUM_RAIN[d] <- out_all$prec[d]
      sfact <- eabyep_calc(soilcp=soilcp,soilsat=soilsat,cropfc=1,avail=0,rain=out_all$prec[d],evap=out_all$ETMAX[d])
      out_all$AVAIL[d] <- sfact$AVAIL
      out_all$ERATIO[d] <- sfact$ERATIO
      out_all$LOGGING[d] <- sfact$LOGGING
      out_all$RUNOFF[d] <- sfact$RUNOFF
      out_all$DEMAND[d] <- sfact$DEMAND
      
    } else {
      out_all$CUM_RAIN[d] <- out_all$CUM_RAIN[d-1] + out_all$prec[d]
      sfact <- eabyep_calc(soilcp=soilcp,soilsat=soilsat,cropfc=1,avail=out_all$AVAIL[d-1],rain=out_all$prec[d],evap=out_all$ETMAX[d])
      out_all$AVAIL[d] <- sfact$AVAIL
      out_all$ERATIO[d] <- sfact$ERATIO
      out_all$LOGGING[d] <- sfact$LOGGING
      out_all$RUNOFF[d] <- sfact$RUNOFF
      out_all$DEMAND[d] <- sfact$DEMAND
    }
  }
  return(out_all)
}

# calculate number of water stress days
calc_wsdays <- function(ERATIO, season_ini=1, season_end=365, e_thresh=0.3) {
  wsdays <- length(which(ERATIO[season_ini:season_end] < e_thresh))
  return(wsdays)
}
calc_wsdaysCMP <- compiler::cmpfun(calc_wsdays)

### HTS1, HTS2, LETHAL: heat stress using tmax ***
calc_hts <- function(tmax, season_ini=1, season_end=365, t_thresh=35) {
  hts <- length(which(tmax[season_ini:season_end] >= t_thresh))
  return(hts)
}
calc_htsCMP <- compiler::cmpfun(calc_hts)

### CD: crop duration, if Tmean > (22, 23, 24) then CD=T-23, else CD=0 ***
calc_cdur <- function(TMEAN, season_ini=1, season_end=365, t_thresh=35){
  tmean <- mean(TMEAN[season_ini:season_end], na.rm=T)
  if (tmean > t_thresh) {cdur <- tmean - t_thresh} else {cdur <- 0}
  return(cdur)
}
calc_cdurCMP <- compiler::cmpfun(calc_cdur)

# DS2: max number of consecutive days Ea/Ep < 0.4, 0.5, 0.6
calc_cons_wsdays <- function(x, season_ini=1, season_end=365, e_thresh=0.4) {
  cdd <- 0; cdd_seq <- c()
  for (i_x in season_ini:season_end) {
    if (x$ERATIO[i_x] < e_thresh) {
      cdd <- cdd+1
    } else {
      cdd_seq <- c(cdd_seq, cdd)
      cdd <- 0
    }
  }
  cdd_seq <- c(cdd_seq, cdd)
  max_cdd <- max(cdd_seq)
  return(max_cdd)
}

# ATT: accum thermal time using capped top, Tb=7,8,9, To=30,32.5,35
calc_att <- function(x, season_ini=1, season_end=365, tb=10, to=20) {
  x$TMEAN <- (x$tmin + x$tmax) * 0.5
  att <- sapply(x$TMEAN[season_ini:season_end], ttfun, tb, to)
  att <- sum(att,na.rm=T)
  return(att)
}

# function to calc tt
ttfun <- function(tmean, tb, to) {
  if (tmean<to & tmean>tb) {
    teff <- tmean-tb
  } else if (tmean>=to) {
    teff <- to-tb
  } else if (tmean<=tb) {
    teff <- 0
  }
  return(teff)
}

# DLOSS: duration loss (difference between No. days to reach ATT_baseline in future vs. baseline)
calc_dloss <- function(x, season_ini, dur_b=110, att_b=5000, tb=10, to=20) {
  x$TMEAN <- (x$tmin + x$tmax) * 0.5
  att <- sapply(x$TMEAN[season_ini:(nrow(x))], ttfun, tb, to)
  att <- cumsum(att)
  estdur <- length(att[which(att < att_b)])
  dloss <- dur_b - estdur
  return(dloss)
}

# WES: wet early season if period between sowing and anthesis is above field cap. >= 50 % time
#      i.e. frequency of days if RUNOFF > 1
calc_wes <- function(x, season_ini, season_end, r_thresh=1) {
  wes <- length(which(x$RUNOFF[season_ini:season_end] > r_thresh))
  return(wes)
}

# BADSOW: no. days in sowing window +-15 centered at sdate with 0.05*SOILCP < AVAIL < 0.9*SOILCP
#         if this is < 3 then crop runs into trouble
calc_badsow <- function(x, season_ini, soilcp) {
  sow_i <- season_ini - 15; sow_f <- season_ini + 15
  if (sow_i < 1) {sow_i <- 1}; if (sow_f > 365) {sow_f <- 365}
  x <- x[sow_i:sow_f,]
  badsow <- length(which(x$AVAIL > (0.05*soilcp) & x$AVAIL < (0.9*soilcp)))
  return(badsow)
}

# BADHAR: no. days in harvest window (+25 after hdate) with AVAIL < 0.85*SOILCP
#         if this is < 3 then crop runs into trouble
calc_badhar <- function(x, season_end, soilcp) {
  har_i <- season_end
  har_f <- har_i + 25; if (har_f > 365) {har_f <- 365}
  x <- x[har_i:har_f,]
  badhar <- length(which(x$AVAIL < (0.85*soilcp)))
  return(badhar)
}

# NWLD: Number of days during the growing season with waterlogging in the soil
calc_NWLD <- function(LOGG = df$LOGGING){
  NWLD <- sum(LOGG > 0, na.rm = T)
  return(NWLD)
}
calc_NWLDMP <- compiler::cmpfun(calc_NWLD)

# NWLD50: Number of days during the growing season with 50% of waterlogging in the soil
calc_NWLD50 <- function(LOGG = df$LOGGING, sat = soilst){
  NWLD50 <- sum(LOGG > (sat*0.5), na.rm = T)
  return(NWLD50)
}
calc_NWLD50MP <- compiler::cmpfun(calc_NWLD50)

# NWLD90: Number of days during the growing season with 90% of waterlogging in the soil
calc_NWLD90 <- function(LOGG = df$LOGGING, sat = soilst){
  NWLD90 <- sum(LOGG >= sat, na.rm = T)
  return(NWLD90)
}
calc_NWLD90MP <- compiler::cmpfun(calc_NWLD90)

# cNWLD Maximum number of consecutive days during the growing season with waterlogging in the soil
calc_cNWLD <- function(LOGG = df$LOGGING){
  runs <- rle(LOGG > 0)
  cNWLD <- max(runs$lengths[runs$values==1], na.rm = TRUE)
  return(cNWLD)
}
calc_cNWLDMP <- compiler::cmpfun(calc_cNWLD)

# cNWLD50 Maximum number of consecutive days during the growing season with 50% of waterlogging in the soil
calc_cNWLD50 <- function(LOGG = df$LOGGING, sat = soilst){
  runs <- rle(LOGG > (sat*0.5))
  cNWLD50 <- max(runs$lengths[runs$values==1], na.rm = TRUE)
  return(cNWLD50)
}
calc_cNWLD50MP <- compiler::cmpfun(calc_cNWLD50)

# cNWLD90 Maximum number of consecutive days during the growing season with 90% of waterlogging in the soil
calc_cNWLD90 <- function(LOGG = df$LOGGING, sat = soilst){
  runs <- rle(LOGG >= sat)
  cNWLD90 <- max(runs$lengths[runs$values==1], na.rm = TRUE)
  return(cNWLD90)
}
calc_cNWLD90MP <- compiler::cmpfun(calc_cNWLD90)

# Aridity index
calc_tai <- function(clm = tbl){
  
  OSys <<- Sys.info()[1]
  root <<- switch(OSys,
                  'Linux'   = '/dapadfs/workspace_cluster_14/WFP_ClimateRiskPr',
                  'Windows' = '//CATALOGUE/Workspace14/WFP_ClimateRiskPr')
  
  # Load packages
  if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
  suppressMessages(pacman::p_load(fst,envirem,gtools,tidyverse,raster))
  
  # Load CHIRPS template
  tmp <- raster::raster(paste0(root,"/1.Data/chirps-v2.0.2020.01.01.tif"))
  
  # Transform table to raster study area
  r <- raster::rasterFromXYZ(xyz = clm[,c('x','y')] %>% unique %>% dplyr::mutate(vals = 1),
                             res = raster::res(tmp),
                             crs = raster::crs(tmp))
  
  # ET SRAD
  srf <- list.dirs(paste0(root,'/1.Data/climate/ET_SolRad'), full.names = T, recursive = F)
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
  names(TAI) <- paste0('TAI_',range(summ$Year)[1]:range(summ$Year)[2])
  df <- TAI %>% raster::rasterToPoints() %>% as.data.frame()
  df$id <- raster::cellFromXY(tmp, df[,c('x','y')])
  df <- df %>% dplyr::select(id,x,y,dplyr::everything(.))
  df <- df %>%
    tidyr::pivot_longer(cols = paste0('TAI_',range(summ$Year)[1]):paste0('TAI_',range(summ$Year)[2]), names_to = 'Year', values_to = 'TAI') %>%
    dplyr::mutate(Year = gsub('TAI_','',Year) %>% as.numeric)
  return(df)
}
calc_taiMP <- compiler::cmpfun(calc_tai)

# Annual/Season total precipitation (ATR) 
calc_atr <- function(PREC){
  pre <- sum(PREC, na.rm = T)
  return(pre)
}
calc_atrMP <- compiler::cmpfun(calc_atr)

# Annual/Season mean temperature --- AMT
calc_amt <- function(TMEAN){
  tavg <- mean(TMEAN, na.rm = T)
  return(tavg)
}
calc_amtMP <- compiler::cmpfun(calc_amt)

# Standardized precipitation index - SPI
calc_spi <- function(DP){
  sp <- SPEI::spi(data = DP, scale = 4, na.rm=T)
  return(sp)
}
calc_spiMP <- compiler::cmpfun(calc_spi)

# The SHI is computed as the number of days with maximum temperatures ??? 29?C 
# and relative humidity is greater than 50%.
calc_SHI <- function(tmax, RH){
  SHI <- ifelse(tmax >= 29 & RH > 50, 1, 0)
  SHI <- sum(SHI)
  return(SHI)
}
calc_SHIMP <- compiler::cmpfun(calc_SHI)

# Daily pig heat stress index (HSI) 
# dplyr::case_when(0 ~ normal, 1 ~ alert, 2 ~ danger, 3 ~ emergency)
calc_HSI <- function(tmax, RH){
  HSI <- tibble::tibble( HSI = case_when( tmax < 24 ~ 0, 
                                          tmax <= 24 & RH <= 70 ~ 0, 
                                          tmax <= 25 & RH <= 40 ~ 0,
                                          tmax <= 24 & RH > 70 ~ 1,
                                          tmax <= 25 & RH > 40 ~ 1,
                                          tmax <= 26 & RH <= 70 ~ 1,
                                          tmax <= 27 & RH <= 40 ~ 1, #
                                          tmax <= 26 & RH > 70 ~ 2,
                                          tmax <= 27 & RH >= 40 & RH < 85 ~ 2, # 
                                          tmax <= 28 & RH < 85 ~ 2,
                                          tmax <= 29 & RH < 60 ~ 2,
                                          tmax <= 30 & RH < 40 ~ 2,
                                          tmax <= 27 & RH >= 85 ~ 3,
                                          tmax <= 28 & RH >= 85 ~ 3,
                                          tmax <= 29 & RH >= 60 ~ 3,
                                          tmax <= 30 & RH >= 40 ~ 3,
                                          tmax > 30 ~ 3,  
                                          TRUE ~ NA_real_) )
  
  if(is.na(sum(HSI))){
    HSI <- tibble(HSI_0 = NA_real_, HSI_1 = NA_real_, HSI_2 = NA_real_, HSI_3 = NA_real_)
  } else {
    HSI <- HSI %>% 
      dplyr::count(HSI) %>%
      dplyr::mutate(n = n / sum(n),
                    HSI = paste0('HSI_', HSI)) %>%
      tidyr::pivot_wider(names_from = HSI, values_from = n)
  }
  HSI_names <- HSI %>% names()
  if(sum(HSI_names == 'HSI_0') < 1){HSI <- bind_cols( tibble(HSI_0 = NA_real_), HSI)}
  if(sum(HSI_names == 'HSI_1') < 1){HSI <- bind_cols( tibble(HSI_1 = NA_real_), HSI)}
  if(sum(HSI_names == 'HSI_2') < 1){HSI <- bind_cols( tibble(HSI_2 = NA_real_), HSI)}
  if(sum(HSI_names == 'HSI_3') < 1){HSI <- bind_cols( tibble(HSI_3 = NA_real_), HSI)}
  HSI <- dplyr::select(HSI, HSI_0, HSI_1, HSI_2, HSI_3)
  return(HSI)
}
calc_HSIMP <- compiler::cmpfun(calc_HSI)

# Daily thermal humidity index (THI)
calc_THI <- function(tmax, RH){
  THI <- tibble::tibble(THI1 = (1.8 * tmax + 32) - ((0.55 - 0.0055 * RH) * (1.8 * tmax - 26.8))) %>% 
    dplyr::mutate(THI = dplyr::case_when(THI1 < 72 ~ 0, # no stress.
                                         THI1 >= 72 & THI1 < 79 ~ 1, # mild
                                         THI1 >= 79 & THI1 < 89 ~ 2, # moderate.
                                         THI1 >= 89 ~ 3, # severe.
                                         TRUE ~ THI1) ) %>%
    dplyr::count(THI)
  
  if(nrow(THI) < 4){
    less <- (0:3)[!(0:3 %in% THI$THI)]
    THI <- dplyr::add_row(THI, tibble(THI = less, n = 0)) %>% arrange(THI)
  } else {
    THI <- THI
  }
  
  THI <- THI %>% dplyr::filter(!is.na(THI)) %>%
    dplyr::mutate(n = ifelse( n > 0, n/sum(n), 0), THI = paste0('THI_', THI)) %>% 
    tidyr::pivot_wider(names_from = THI, values_from = n)
  
  if(sum(slice(THI, 1) ) == 0){
    THI <- THI %>%
      dplyr::mutate_all(.funs = function(x){
        ifelse(x == 0, NA_real_, x)
      })}
  THI_names <- THI %>% names()
  if(sum(THI_names == 'THI_0') < 1){THI <- bind_cols( tibble(THI_0 = NA_real_), THI)}
  if(sum(THI_names == 'THI_1') < 1){THI <- bind_cols( tibble(THI_1 = NA_real_), THI)}
  if(sum(THI_names == 'THI_2') < 1){THI <- bind_cols( tibble(THI_2 = NA_real_), THI)}
  if(sum(THI_names == 'THI_3') < 1){THI <- bind_cols( tibble(THI_3 = NA_real_), THI)}
  THI <- dplyr::select(THI, THI_0, THI_1, THI_2, THI_3)
  return(THI)
}
calc_THIMP <- compiler::cmpfun(calc_THI)

## CSDI
calc_csdi <- function(TMIN){
  per_10 <- quantile(TMIN, 0.10, na.rm = T)
  runs <- rle(TMIN < per_10)
  cons_days <- max(runs$lengths[runs$value==1], na.rm=T)
  cons_days <- ifelse(cons_days<6,0,cons_days)
  return(cons_days)
}
calc_csdiMP <- compiler::cmpfun(calc_csdi)
