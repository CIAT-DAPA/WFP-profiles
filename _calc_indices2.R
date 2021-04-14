# -------------------------------------------------- #
# Climate Risk Profiles -- Agro-climatic indices calculation
# A. Esquivel, C. Saavedra, H. Achicanoy & J. Ramirez-Villegas
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

options(warn = -1, scipen = 999)
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyft,SPEI,tidyverse,raster,ncdf4,sf,future,furrr,lubridate,glue,vroom,sp,fst,compiler))

# Input parameters:
#   climate: path or data frame with the climate data. This file must exists
#   soil: soil file path. This file must exists
#   seasons: list object specifying the months where a season take
#     place. Examples:
#       One season:  seasons = list(s1 = 2:5) # Feb-May
#       Two seasons: seasons = list(s1 = 2:5, s2 = c(10:12,1)) # Feb-May and Oct-Jan
#   subset: logical value. If TRUE, the indices will be calculated
#     for a random sample of 30% of the pixels. If FALSE, all the
#     pixels will be used
#   ncores: number of cores to run the code in parallel per pixel
#   outfile: output file path (all indices)
#   spi_out: output file path (SPI index)
# Output:
#   Two data.frames one with all the agro-climatic indices and another
#   with the SPI index values for all pixels
calc_indices2 <- function(climate = infile,
                          soil    = soilfl,
                          seasons = list(s1 = mnth), # list(s1 = 2:6, s2 = 10:12)
                          ncores  = 10,
                          outfile = outfile,
                          spi_out = spi_out){
  
  if(!file.exists(outfile)){
    dir.create(path = dirname(outfile), FALSE, TRUE)
    
    # Load soil data
    Soil <- soil %>%
      tidyft::parse_fst(path = .) %>%
      tidyft::select_fst(id,x,y,scp,ssat) %>%
      base::as.data.frame()
    # Impute missing data using the nearest neighbor
    if(nrow(Soil[is.na(Soil$scp),]) > 0){
      NAs <- Soil[is.na(Soil$scp),]
      for(i in 1:nrow(NAs)){
        dst <- geosphere::distm(x = Soil[,c('x','y')], y = NAs[i,c('x','y')]) %>% as.numeric()
        Soil$scp[which(Soil$id == NAs$id[i])] <- Soil$scp[which(as.integer(rank(dst)) == 2)]
        Soil$ssat[which(Soil$id == NAs$id[i])] <- Soil$ssat[which(as.integer(rank(dst)) == 2)]
      }; rm(i, NAs)
    }
    
    # Load climate data
    if(class(climate) == 'character'){
      clim_data <- climate %>%
        tidyft::parse_fst(path = .) %>%
        base::as.data.frame()
    } else {
      clim_data <- climate
    }
    clim_data$year <- NULL
    clim_data <- clim_data %>%
      dplyr::select(id,x,y,date,prec,tmax,tmean,tmin,srad,rh) %>%
      dplyr::mutate(id1 = id) %>%
      tidyr::nest(Climate = c('id','date','prec','tmax','tmean','tmin','srad','rh')) %>% # 'wind'
      dplyr::rename(id = 'id1') %>%
      dplyr::select(id, dplyr::everything(.))
    
    # Match soil and climate pixels
    px <- intersect(clim_data$id, Soil$id)
    clim_data <- clim_data[clim_data$id %in% px,]
    
    # Impute missing data
    impute_missings <- function(tbl = clim_data){
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
          # if(sum(is.na(df$wind)) > 0){
          #   df$wind[which(is.na(df$wind))] <- median(df$wind, na.rm = T)
          # }
          return(df)
        })
      
      return(Climate)
    }
    clim_data$Climate <- impute_missings(tbl = clim_data)
    
    # Calc aridity index for all pixels
    tai <- clim_data %>% tidyr::unnest() %>%
      dplyr::select(-id1) %>% calc_taiMP(clm = .)
    
    # Calc SPI (Standardized Precipitation Index) for all pixels
    if(!file.exists(spi_out)){
      spi <- clim_data %>% tidyr::unnest() %>%
        dplyr::select(-id1) %>%
        dplyr::mutate(year  = lubridate::year(date),
                      month = lubridate::month(date)) %>%
        dplyr::group_by(id, year, month) %>%
        dplyr::summarize(ATR = calc_atrMP(PREC = prec)) %>%
        dplyr::arrange(year, month) %>%
        dplyr::group_by(id) %>%
        dplyr::mutate(SPI = calc_spi(DP = ATR)$fitted) %>%
        setNames(c('id','month','year','ATR','SPI')) %>%
        dplyr::mutate(SPI = as.numeric(SPI))
      fst::write_fst(x = spi, path = spi_out)
    } else {
      cat('SPI index already calculated\n')
    }
    
    # Run indices per pixel
    run_pixel <- function(id = 4566921){
      
      cat(' --- Obtain complete time series per pixel\n')
      tbl <- clim_data$Climate[[which(clim_data$id == id)]]
      tbl <- tbl %>%
        dplyr::mutate(year  = lubridate::year(as.Date(date)),
                      month = lubridate::month(as.Date(date)))
      years <- tbl$year %>% unique
      
      cat(' --- Calculate water balance for complete time series\n')
      soilcp <- Soil$scp[Soil$id == id]
      soilst <- Soil$ssat[Soil$id == id]
      watbal_loc <- watbal_wrapper(tbl, soilcp, soilst)
      watbal_loc$IRR <- watbal_loc$ETMAX - watbal_loc$prec
      
      tbl <- tbl %>%
        dplyr::mutate(ERATIO  = watbal_loc$ERATIO,
                      TAV     = (watbal_loc$tmin + watbal_loc$tmax)/2,
                      IRR     = watbal_loc$IRR,
                      LOGGING = watbal_loc$LOGGING,
                      GDAY    = ifelse(TAV >= 6 & ERATIO >= 0.35, yes = 1, no = 0))
      
      cat(' --- Estimate growing seasons from water balance\n')
      
      ### CONDITIONS TO HAVE IN ACCOUNT
      # Length of growing season per year
      # Start: 5-consecutive growing days.
      # End: 12-consecutive non-growing days.
      
      # Run process by year
      lgp_year_pixel <- lapply(1:length(years), function(k){
        
        # Subsetting by year
        watbal_year <- tbl[tbl$year==years[k],]
        
        # Calculate sequences of growing and non-growing days within year
        runsDF <- rle(watbal_year$GDAY)
        runsDF <- data.frame(Lengths=runsDF$lengths, Condition=runsDF$values)
        runsDF$Condition <- runsDF$Condition %>% tidyr::replace_na(replace = 0)
        
        # Identify start and extension of each growing season during year
        if(!sum(runsDF$Lengths[runsDF$Condition==1] < 5) == length(runsDF$Lengths[runsDF$Condition==1])){
          
          LGP <- 0; LGP_seq <- 0
          for(i in 1:nrow(runsDF)){
            if(runsDF$Lengths[i] >= 5 & runsDF$Condition[i] == 1){
              LGP <- LGP + 1
              LGP_seq <- c(LGP_seq, LGP)
              LGP <- 0
            } else {
              if(LGP_seq[length(LGP_seq)]==1){
                if(runsDF$Lengths[i] >= 12 & runsDF$Condition[i] == 0){
                  LGP <- 0
                  LGP_seq <- c(LGP_seq, LGP)
                } else {
                  LGP <- LGP + 1
                  LGP_seq <- c(LGP_seq, LGP)
                  LGP <- 0
                }
              } else {
                LGP <- 0
                LGP_seq <- c(LGP_seq, LGP)
              }
            }
          }
          LGP_seq <- c(LGP_seq, LGP)
          LGP_seq <- LGP_seq[-c(1, length(LGP_seq))]
          runsDF$gSeason <- LGP_seq; rm(i, LGP, LGP_seq)
          LGP_seq <- as.list(split(which(runsDF$gSeason==1), cumsum(c(TRUE, diff(which(runsDF$gSeason==1))!=1))))
          
          # Calculate start date and extension of each growing season by year and pixel
          growingSeason <- lapply(1:length(LGP_seq), function(g){
            
            LGP_ini <- sum(runsDF$Lengths[1:(min(LGP_seq[[g]])-1)]) + 1
            LGP     <- sum(runsDF$Lengths[LGP_seq[[g]]])
            results <- data.frame(id=tbl$id %>% unique, year=years[k], gSeason=g, SLGP=LGP_ini, LGP=LGP)
            return(results)
            
          })
          growingSeason <- do.call(rbind, growingSeason)
          if(nrow(growingSeason)>2){
            growingSeason <- growingSeason[rank(-growingSeason$LGP) %in% 1:2,]
            growingSeason$gSeason <- rank(growingSeason$SLGP)
            growingSeason <- growingSeason[order(growingSeason$gSeason),]
          }
          
        } else {
          
          growingSeason <- data.frame(id=tbl$id %>% unique, year=years[k], gSeason = 1:2, SLGP = NA, LGP = NA)
          
        }
        
        print(k)
        return(growingSeason)
        
      })
      lgp_year_pixel <- do.call(rbind, lgp_year_pixel); rownames(lgp_year_pixel) <- 1:nrow(lgp_year_pixel)
      
      # if(length(seasons) == 1){
      #   lgp_year_pixel <- lgp_year_pixel %>%
      #     dplyr::filter(gSeason == 1)
      # } else {
      #   if(length(seasons) == 2){
      #     lgp_year_pixel <- lgp_year_pixel %>%
      #       dplyr::filter(gSeason %in% 1:2)
      #   }
      # }
      
      cat(' --- Calculate agro-climatic indices for an specific season\n')
      if(!is.null(seasons)){
        indices <- 1:length(seasons) %>%
          purrr::map(.f = function(i){
            season <- seasons[[i]]
            # Season across two years
            if(sum(diff(season) < 0) > 0){
              pairs     <- NA; for(j in 1:length(years)-1){pairs[j] <- paste0(years[j:(j+1)], collapse = '-')}
              tbl_list  <- lapply(1:(length(years)-1), function(k){
                df <- tbl %>%
                  dplyr::filter(year %in% years[k:(k+1)])
                df$pairs <- paste0(years[k:(k+1)], collapse = '-')
                df1 <- df %>%
                  dplyr::filter(year == years[k] & month %in% season[1]:12)
                df2 <- df %>%
                  dplyr::filter(year == years[k+1] & month %in% 1:season[length(season)])
                df <- rbind(df1, df2); rm(df1, df2)
                return(df)
              })
              tbl_list <- dplyr::bind_rows(tbl_list)
            } else {
              # Season in one year
              tbl_list  <- lapply(1:length(years), function(k){
                df <- tbl %>%
                  dplyr::filter(year %in% years[k])
                df$pairs <- years[k]
                df <- df %>%
                  dplyr::filter(year == years[k] & month %in% season)
                return(df)
              })
              tbl_list <- dplyr::bind_rows(tbl_list)
            }
            
            idx <- tbl_list %>%
              dplyr::group_split(pairs) %>%
              purrr::map(.f = function(df){
                idx <- tibble::tibble(ATR  = calc_atrMP(PREC = df$prec), 
                                      AMT  = calc_amtMP(TMEAN = df$tmean),
                                      NDD  = calc_nddCMP(PREC = df$prec),
                                      P5D  = calc_p5dCMP(PREC = df$prec),
                                      P95  = calc_p95CMP(PREC = df$prec),
                                      NT_X = calc_htsCMP(tmax = df$tmax, t_thresh = 35),
                                      NDWS = calc_wsdays(df$ERATIO, season_ini = 1, season_end = length(df$ERATIO), e_thresh=0.5),
                                      NWLD = calc_NWLDMP(LOGG = df$LOGGING),
                                      NWLD50 = calc_NWLD50MP(LOGG = df$LOGGING, sat = soilst),
                                      NWLD90 = calc_NWLD90MP(LOGG = df$LOGGING, sat = soilst), 
                                      IRR  = sum(df$IRR, na.rm = T),
                                      SHI  = calc_SHIMP(tmax = df$tmax, RH = df$rh),
                                      calc_HSIMP(tmax = df$tmax, RH = df$rh),
                                      calc_THIMP(tmax = df$tmax, RH = df$rh))
                return(idx)
              })
            idx <- dplyr::bind_rows(idx)
            idx$year <- tbl_list$pairs %>% unique
            if(sum(diff(season) < 0) > 0){
              idx$year <- substr(x = idx$year, start = 6, stop = 10) %>% as.numeric
            } else {
              idx$year <- idx$year %>% as.numeric
            }
            idx$season <- names(seasons)[i]
            idx$id <- id
            
            return(idx)
            
          })
        indices <- dplyr::bind_rows(indices)
      }
      all <- dplyr::full_join(x = indices, y = lgp_year_pixel, by = c('id','year')) %>% unique()
      
      return(all)
      
    }
    
    sq <- 1:nrow(clim_data)
    chunks <- chunk(vect = sq, size = 150)
    
    tictoc::tic()
    index_by_pixel <- chunks %>%
      purrr::map(.f = function(chk){
        # Run the process in parallel for all the pixels
        plan(cluster, workers = ncores, gc = TRUE)
        index_by_pixel <- clim_data[chk,] %>%
          dplyr::pull(id) %>%
          furrr::future_map(.x = ., .f = run_pixel) %>%
          dplyr::bind_rows()
        future:::ClusterRegistry("stop")
        gc(reset = T)
        return(index_by_pixel)
      }) %>%
      dplyr::bind_rows()
    tictoc::toc()
    
    index_by_pixel <- index_by_pixel %>%
      dplyr::select(id,season,year,
                    ATR,AMT,NDD,P5D,P95,NT_X,
                    NDWS,NWLD,NWLD50,NWLD90,
                    IRR,SHI,
                    HSI_0,HSI_1,HSI_2,HSI_3,
                    THI_0,THI_1,THI_2,THI_3,
                    gSeason,SLGP,LGP)
    index_by_pixel <- dplyr::left_join(x = clim_data[,c('id','x','y')], y = index_by_pixel, by = 'id')
    index_by_pixel$NWLD[index_by_pixel$NWLD == -Inf] <- 0
    index_by_pixel$NWLD50[index_by_pixel$NWLD50 == -Inf] <- 0
    index_by_pixel$NWLD90[index_by_pixel$NWLD90 == -Inf] <- 0
    index_by_pixel$HSI_0[is.na(index_by_pixel$HSI_0)] <- 0
    index_by_pixel$HSI_1[is.na(index_by_pixel$HSI_1)] <- 0
    index_by_pixel$HSI_2[is.na(index_by_pixel$HSI_2)] <- 0
    index_by_pixel$HSI_3[is.na(index_by_pixel$HSI_3)] <- 0
    index_by_pixel$THI_0[is.na(index_by_pixel$THI_0)] <- 0
    index_by_pixel$THI_1[is.na(index_by_pixel$THI_1)] <- 0
    index_by_pixel$THI_2[is.na(index_by_pixel$THI_2)] <- 0
    index_by_pixel$THI_3[is.na(index_by_pixel$THI_3)] <- 0
    index_by_pixel$NDD  <- round(index_by_pixel$NDD)
    index_by_pixel$NT_X <- round(index_by_pixel$NT_X)
    index_by_pixel$NDWS <- round(index_by_pixel$NDWS)
    index_by_pixel$NWLD <- round(index_by_pixel$NWLD)
    index_by_pixel$NWLD50 <- round(index_by_pixel$NWLD50)
    index_by_pixel$NWLD90 <- round(index_by_pixel$NWLD90)
    index_by_pixel$SHI <- round(index_by_pixel$SHI)
    index_by_pixel$SLGP <- round(index_by_pixel$SLGP)
    index_by_pixel$LGP  <- round(index_by_pixel$LGP)
    
    index_by_pixel <- dplyr::full_join(x = tai %>%
                                         dplyr::rename(year = 'Year') %>%
                                         dplyr::select(id, year, TAI),
                                       y = index_by_pixel,
                                       by = c('id','year'))
    index_by_pixel <- index_by_pixel %>%
      dplyr::select(id,x,y,season,year,dplyr::everything(.))
    fst::write_fst(x = index_by_pixel, path = outfile)
    cat('>>> File created successfully ...\n')
  } else {
    cat('>>> File exists it is not necessary to create it again\n')
  }
}
