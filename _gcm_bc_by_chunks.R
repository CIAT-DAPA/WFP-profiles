# -------------------------------------------------- #
# Climate Risk Profiles -- CMIP6 data: GCM bias-correction performed by Quantile-mapping by chunks
# H. Achicanoy, A. Ghosh & C. Navarro
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

options(warn = -1, scipen = 999)
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse,tidyft))

source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_main_functions.R') # Main functions
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_gcm_bc.R')         # GCMs bias-correction

root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'

iso     <- 'TZA'
model   <- 'INM-CM5-0'
period  <- '2041-2060'
his_obs <- paste0(root,"/1.Data/observed_data/",iso,"/",iso,".fst")
his_gcm <- paste0(root,"/1.Data/future_data/",model,"/",iso,"/downscale/1995-2014/",iso,".fst")
fut_gcm <- paste0(root,"/1.Data/future_data/",model,"/",iso,"/downscale/",period,"/",iso,".fst")

# Historical climate data (observed)
ho <- his_obs %>%
  tidyft::parse_fst() %>%
  tidyft::select_fst(id) %>%
  tidyft::distinct() %>%
  base::as.data.frame()
ho <- ho$id
# Historical GCM climate data (modeled)
hg <- his_gcm %>%
  tidyft::parse_fst() %>%
  tidyft::select_fst(id) %>%
  tidyft::distinct() %>%
  base::as.data.frame()
hg <- hg$id
# Furure GCM climate data (modeled)
fg <- fut_gcm %>%
  tidyft::parse_fst() %>%
  tidyft::select_fst(id) %>%
  tidyft::distinct() %>%
  base::as.data.frame()
fg <- fg$id

# Indentify pixels intersection
px <- base::intersect(ho, hg)
px <- base::intersect(px, fg)
px <- sort(px); rm(ho, hg, fg)

chnks <- chunk(px, 4000)

1:length(chnks) %>%
  purrr::map(.f = function(j){
    
    pft <<- chnks[[j]]
    
    his_obs <- his_obs %>%
      tidyft::parse_fst(path = .) %>%
      tidyft::filter_fst(id %in% pft) %>%
      base::as.data.frame()
    
    his_gcm <- his_gcm %>%
      tidyft::parse_fst(path = .) %>%
      tidyft::filter_fst(id %in% pft) %>%
      base::as.data.frame()
    
    fut_gcm <- fut_gcm %>%
      tidyft::parse_fst(path = .) %>%
      tidyft::filter_fst(id %in% pft) %>%
      base::as.data.frame()
    
    his_bc  <- paste0(root,"/1.Data/future_data/",model,"/",iso,"/bias_corrected/1995-2014/",iso,"_chunk_",j,".fst")
    fut_bc  <- paste0(root,"/1.Data/future_data/",model,"/",iso,"/bias_corrected/",period,"/",iso,"_chunk_",j,".fst")
    
    BC_Qmap(his_obs = his_obs,
            his_gcm = his_gcm,
            fut_gcm = fut_gcm,
            his_bc  = his_bc,
            fut_bc  = fut_bc,
            period  = period,
            ncores  = 1)
    
    return(cat(paste0('Chunk ',j,' finished\n')))
    
  })
