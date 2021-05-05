# -------------------------------------------------- #
# Climate Risk Profiles -- Master code
# A. Esquivel, C. Saavedra, H. Achicanoy & J. Ramirez-Villegas
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# Sourcing functions
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_main_functions.R')         # Main functions
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_get_soil_data.R')          # Get soil data
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_calc_indices.R')           # Calculating agro-indices
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_calc_indices2.R')          # Calculating agro-indices
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_get_climate4regions.R')    # Filter climate for areas of interest
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_calc_spi_drought.R')       # SPI calculation
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/time_series_plot.R')        # Time series graphs
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/maps.R')                    # Maps
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/time_series_plot_region.R') # Time series graphs by region
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/climatology_plot.R')        # Climatology graph. 
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/elv_map.R')                 # Elevation map. 

root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'

## Defining country parameters
# Country
country <- 'Guinea-Bissau'
iso     <- 'GNB'
seasons <- list(s1=1,s2=2,s3=3,s4=4,s5=5,s6=6,s7=7,s8=8,s9=9,s10=10,s11=11,s12=12)

# Get historical climate data (done)

# Get soil data
crd <- paste0(root,"/1.Data/observed_data/",iso,"/year/climate_1981_mod.fst") %>%
  tidyft::parse_fst(path = .) %>%
  tidyft::select_fst(id,x,y) %>%
  base::as.data.frame()
crd <- unique(crd[,c('id','x','y')])
get_soil(crd        = crd,
         root_depth = 60,
         outfile    = paste0(root,"/1.Data/soil/",iso,"/soilcp_data.fst"))

# Get future climate data

# Calc agro-climatic indices (past)
infile  <- paste0(root,"/1.Data/observed_data/",iso,"/",iso,".fst") # infile <- flt_clm(iso = iso, country = country)
soilfl  <- paste0(root,"/1.Data/soil/",iso,"/soilcp_data.fst")
outfile <- paste0(root,"/7.Results/",country,"/past/",iso,"_indices.fst")
spi_out <- paste0(root,"/7.Results/",country,"/past/",iso,"_spi.fst")
calc_indices(climate = infile,
             soil    = soilfl,
             seasons = seasons,
             subset  = F,
             ncores  = 15,
             outfile = outfile,
             spi_out = spi_out)

# # How much area per municipality is on average subject to ‘Major droughts’ (SPI < -1.5)
# infile  <- paste0(root,"/7.Results/",country,"/past/",iso,"_spi.fst")
# outfile <- paste0(root,'/7.Results/',country,'/past/',iso,'_spi_drought.fst')
# calc_spi_drought(spi_data = infile,
#                  output   = outfile,
#                  country  = country,
#                  iso      = iso,
#                  seasons  = seasons)

# Calc agro-climatic indices (future)
models  <- c('INM-CM5-0', 'GFDL-ESM4','MPI-ESM1-2-HR','MRI-ESM2-0','BCC-CSM2-MR') # 'GFDL-ESM4','MPI-ESM1-2-HR','MRI-ESM2-0','BCC-CSM2-MR'
periods <- c('2021-2040','2041-2060')
for(m in models){
  for(p in periods){
    infile  <- paste0(root,"/1.Data/future_data/",m,"/",iso,"/bias_corrected/",p,"/",iso,".fst")
    soilfl  <- paste0(root,"/1.Data/soil/",iso,"/soilcp_data.fst")
    outfile <- paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_indices_monthly.fst")
    spi_out <- paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_spi.fst")
    calc_indices(climate = infile,
                 soil    = soilfl,
                 seasons = seasons,
                 subset  = F,
                 ncores  = 15,
                 outfile = outfile,
                 spi_out = spi_out)
  }
}

## Graphs
# 1. Time series plots
time_series_plot(country = country, iso = iso, seasons = seasons)

# 2. Time series plots by zone 
time_series_region(country = country, iso = iso, seasons = seasons)
