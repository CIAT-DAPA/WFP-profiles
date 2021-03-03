# -------------------------------------------------- #
# Climate Risk Profiles -- Master code Haiti
# A. Esquivel, C. Saavedra, H. Achicanoy & J. Ramirez-Villegas
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# Sourcing functions
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_main_functions.R') # Main functions
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_get_soil_data.R')  # Get soil data
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_calc_indices.R')   # Calculating agro-indices

root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'

## Defining country parameters
# Country
country <- 'Haiti'
iso3    <- 'HTI'

# Livelihood zones
lhzs <- c('','','','')

# Get historical climate data
# Aleja function to extract and reorganize the data in table format
# If data is already processed, just load the table or path

# Get soil data
crd <- fst::read_fst(paste0(root,"/1.Data/observed_data/",iso3,"/",iso3,".fst"))
crd <- unique(crd[,c('id','x','y')])
get_soil(crd        = crd,
         root_depth = 60,
         outfile    = paste0(root,"/1.Data/soil/",iso3,"/soilcp_data.fst"))

# Get future climate data
# Same as previous function

# Crops
crps <- c('maize','pulses','cassava')

# Crop calendar
mnth <- 6:12 # Manually
# mnth <- get_crop_calendar_from_crops() # Cesar's function
# mnth <- get_crop_calendar_from_rains() # Growing seasons

# Calc agro-climatic indices
infile  <- paste0(root,"/1.Data/observed_data/",iso3,"/",iso3,".fst")
soilfl  <- paste0(root,"/1.Data/soil/",iso3,"/soilcp_data.fst")
outfile <- paste0(root,"/7.Results/",country,"/past/",iso3,"_indices.fst")
spi_out <- paste0(root,"/7.Results/",country,"/past/",iso3,"_spi.fst")
calc_indices(climate = infile,
             soil    = soilfl,
             seasons = list(s1 = 6:12),
             subset  = F,
             ncores  = 15,
             outfile = outfile,
             spi_out = spi_out)

# # Burundi
# infile  <- "//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/observed_data/BDI/BDI.fst"
# soilfl  <- "//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/soil/BDI/soilcp_data.fst"
# outfile <- "//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/Burundi/past/BDI_indices.fst"
# spi_out <- "//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/Burundi/past/BDI_spi.fst"
# calc_indices(climate = infile,
#              soil    = soilfl,
#              seasons = list(s1 = 2:7, s2 = c(9:12,1)),
#              subset  = F,
#              ncores  = 15,
#              outfile = outfile,
#              spi_out = spi_out)

# Graphs
# 1. Put all together: time series, maps, and climatology graphs
# 2. How much area per municipality is on average subject to ‘Major droughts’ (SPI < -1.5) 
# 3. Categorize hazard layers by absolute ranges
# 4. Using 3. combine hazard layers
list.files('root/results/HTI', full.names = T) %>% # Graphs per livelihood zones
  purrr::map(.f = function(pth){
    df <- fst::read_fst()
    graphs(...)
  })
list.files('root/results/HTI', full.names = T) %>% # Graphs per country
  purrr::map(.f = function(pth){
    df <- fst::read_fst()
  })%>%
  dplyr::bind_rows() %>%
  graphs(...)
