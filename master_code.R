# -------------------------------------------------- #
# Climate Risk Profiles -- Master code
# A. Esquivel, C. Saavedra, H. Achicanoy & J. Ramirez-Villegas
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# Sourcing functions
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_main_functions.R')      # Main functions
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_get_soil_data.R')       # Get soil data
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_calc_indices.R')        # Calculating agro-indices
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_get_climate4regions.R') # Filter climate for areas of interest
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/time_series_plot.R')     # Time series graphs

root <- '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr'

## Defining country parameters
# Country
country <- 'Somalia'
iso     <- 'SOM'
seasons <- list(s1 = 4:8, s2 = c(9:12,1:2))

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

# Calc agro-climatic indices (future)
models  <- c('INM-CM5-0') # 'GFDL-ESM4','MPI-ESM1-2-HR','MRI-ESM2-0','BCC-CSM2-MR'
periods <- c('2021-2040','2041-2060')
for(m in models){
  for(p in periods){
    infile  <- paste0(root,"/1.Data/future_data/",m,"/",iso,"/bias_corrected/",p,"/",iso,".fst")
    soilfl  <- paste0(root,"/1.Data/soil/",iso,"/soilcp_data.fst")
    outfile <- paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_indices.fst")
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

# Time series plots
time_series_plot(country = country, iso = iso)
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

tidyfst::parse_fst(path = '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/observed_data/TZA/TZA.fst') %>%
  tidyfst::slice_fst(ft = ., row_no = c(1,10,20:30))
