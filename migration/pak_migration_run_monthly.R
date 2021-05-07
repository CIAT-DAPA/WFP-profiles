# -------------------------------------------------- #
# Climate Risk Profiles -- Master code
# A. Esquivel, C. Saavedra, H. Achicanoy & J. Ramirez-Villegas
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# Sourcing functions
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_main_functions.R')      # Main functions
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_get_soil_data.R')       # Get soil data
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_calc_indices.R')        # Calculating agro-indices
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_calc_indices2.R')       # Calculating agro-indices
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_get_climate4regions.R') # Filter climate for areas of interest
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/time_series_plot.R')     # Time series graphs
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_calc_spi_drought.R')    # SPI calculation
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/maps.R')                 # Maps
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/migration/_get_climate4regions_districts.R') # Filter climate for districts of interest

flt_clm_subunits <- function(iso = 'SOM', country = 'Somalia', district = 'Caabudwaaq'){
  
  # Load packages
  if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
  suppressMessages(pacman::p_load(tidyverse,raster,terra,tidyft,sf))
  
  root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'
  
  cat('>>> Load all coords\n')
  crd <- paste0(root,'/1.Data/observed_data/',iso,'/year/climate_1981_mod.fst')
  crd <- crd %>%
    tidyft::parse_fst(path = .) %>%
    tidyft::select_fst(id, x, y) %>%
    tidyft::distinct() %>%
    base::as.data.frame()
  
  cat('>>> Regions shape and add a buffer of 5 km\n')
  shp <- sf::read_sf(paste0(root,'/1.Data/others/shps/',tolower(country),'/',tolower(district),'.shp'))
  shp <- sf::st_buffer(shp, dist = 0.05) %>% sf::st_union(.) %>% sf::as_Spatial() %>% terra::vect()
  ref <- terra::rast(paste0(root,'/1.Data/chirps-v2.0.2020.01.01.tif'))
  ref <- terra::crop(ref, terra::ext(shp))
  rst <- terra::rasterize(x = shp, y = ref)
  
  cat('>>> Filter coords in regions of interest\n')
  crd$sl <- terra::extract(x = rst, y = crd[,c('x','y')]) %>% unlist() %>% as.numeric()
  crd <- crd[complete.cases(crd),]
  pft <<- crd$id
  
  cat('>>> Filter climate table to filtered coords\n')
  clm <- paste0(root,'/1.Data/observed_data/',iso,'/',iso,'.fst')
  clm <- clm %>%
    tidyft::parse_fst(path = .) %>%
    tidyft::filter_fst(id %in% pft) %>%
    base::as.data.frame()
  
  return(clm)
  
}

OSys <- Sys.info()[1]
root <<- switch(OSys,
                'Linux'   = '/dapadfs/workspace_cluster_13/WFP_ClimateRiskPr',
                'Windows' = '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr')

## Defining country parameters
# Country
country <- 'Pakistan'
iso     <- 'PAK'
seasons <- list(s1=1,s2=2,s3=3,s4=4,s5=5,s6=6,s7=7,s8=8,s9=9,s10=10,s11=11,s12=12)

# # Get historical climate data (done)
# 
# # Get soil data
# crd <- paste0(root,"/1.Data/observed_data/",iso,"/year/climate_1981_mod.fst") %>%
#   tidyft::parse_fst(path = .) %>%
#   tidyft::select_fst(id,x,y) %>%
#   base::as.data.frame()
# crd <- unique(crd[,c('id','x','y')])
# get_soil(crd        = crd,
#          root_depth = 60,
#          outfile    = paste0(root,"/1.Data/soil/",iso,"/soilcp_data.fst"))
# 
# # Get future climate data

# Calc agro-climatic indices (past)
districts <- c("Malakand P.A.","Lahore","Sibi","Karachi East",
               "Panjgur","Jafarabad","Tando M. Khan","Okara",
               "Okara 1","Tank","Tando Allahyar","Faisalabad",
               "Chitral","Sheikhupura","Gilgit",
               "Gilgit (Tribal Territory)","Kohat","Rajan Pur",
               "Larkana","Thatta","Sargodha","Sanghar","Khushab",
               "Changai","Multan","Kargil","Islamabad","Umerkot",
               "Bruner","Swat","Mastung","Dera Ghazi Kha")
for(i in 1:length(districts)){
  
  soilfl  <- paste0(root,"/1.Data/soil/",iso,"/soilcp_data.fst")
  outfile <- paste0(root,"/1.Data/others/",country,"/past/",tolower(districts[i]),"_indices_monthly.fst")
  spi_out <- paste0(root,"/1.Data/others/",country,"/past/",tolower(districts[i]),"_spi.fst")
  
  if(!file.exists(outfile)){
    
    cat(paste0('Processing district: ',districts[i],'\n'))
    
    infile <- flt_clm_subunits(iso = iso, country = country, district = districts[i])
    tryCatch(expr={
      calc_indices(climate = infile,
                   soil    = soilfl,
                   seasons = seasons,
                   subset  = F,
                   ncores  = 40,
                   outfile = outfile,
                   spi_out = spi_out)
    },
    error=function(e){
      cat(paste0("Modeling process failed in district: ",districts[i],"\n"))
      return("Done\n")
    })
    
    cat(paste0('District: ',districts[i],' finished successfully\n'))
    
  } else {
    cat(paste0('Monthly indices are already calculated for district: ',districts[i],'\n'))
  }
  
}


# # How much area per municipality is on average subject to ‘Major droughts’ (SPI < -1.5)
# infile  <- paste0(root,"/7.Results/",country,"/past/",iso,"_spi.fst")
# outfile <- paste0(root,'/7.Results/',country,'/past/',iso,'_spi_drought.fst')
# calc_spi_drought(spi_data = infile,
#                  output   = outfile,
#                  country  = country,
#                  iso      = iso,
#                  seasons  = seasons)
# 
# # Calc agro-climatic indices (future)
# models  <- c('INM-CM5-0') # 'GFDL-ESM4','MPI-ESM1-2-HR','MRI-ESM2-0','BCC-CSM2-MR'
# periods <- c('2021-2040','2041-2060')
# for(m in models){
#   for(p in periods){
#     infile  <- paste0(root,"/1.Data/future_data/",m,"/",iso,"/bias_corrected/",p,"/",iso,".fst")
#     soilfl  <- paste0(root,"/1.Data/soil/",iso,"/soilcp_data.fst")
#     outfile <- paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_indices.fst")
#     spi_out <- paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_spi.fst")
#     calc_indices(climate = infile,
#                  soil    = soilfl,
#                  seasons = seasons,
#                  subset  = F,
#                  ncores  = 15,
#                  outfile = outfile,
#                  spi_out = spi_out)
#     infile  <- paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_spi.fst")
#     outfile <- paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_spi_drought.fst")
#     calc_spi_drought(spi_data = infile,
#                      output   = outfile,
#                      country  = country,
#                      iso      = iso,
#                      seasons  = seasons)
#   }
# }
# 
# ## Graphs
# # 1. Time series plots
# time_series_plot(country = country, iso = iso, seasons = seasons)
# 
# # 2. Climatology
# 
# # 3. Elevation map
# 
# # 4. Maps
# map_graphs(iso3 = iso, country = country, seasons = seasons, Zone = 'all')
# 
# # 6. PPT slides
