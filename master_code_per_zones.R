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
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/_calc_spi_drought.R')       # SPI calculation
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/time_series_plot.R')        # Time series graphs
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/maps.R')                    # Maps
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/time_series_plot_region.R') # Time series graphs by region
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/climatology_plot.R')        # Climatology graph. 
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/elv_map.R')                 # Elevation map. 
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/summary.R')                 # summary indices (mean, median...)
source('https://raw.githubusercontent.com/CIAT-DAPA/WFP-profiles/main/migration/_get_climate4regions_districts.R') # Filter climate for districts of interest

OSys <<- Sys.info()[1]
root <<- switch(OSys,
                'Linux'   = '/dapadfs/workspace_cluster_14/WFP_ClimateRiskPr',
                'Windows' = '//dapadfs.cgiarad.org/workspace_cluster_14/WFP_ClimateRiskPr')

## Defining country parameters
# Country
country <- 'Guinee'
iso     <- 'GIN'
seasons <- list(s1 = 4:12)

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
districts <- list.files(path = paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_zones'), pattern = '.shp$', full.names = F) %>% gsub('.shp','',.) %>% gtools::mixedsort()
for(i in 1:length(districts)){
  
  soilfl  <- paste0(root,"/1.Data/soil/",iso,"/soilcp_data.fst")
  outfile <- paste0(root,"/7.Results/",country,"/past/",districts[i],"_indices.fst")
  spi_out <- paste0(root,"/7.Results/",country,"/past/",districts[i],"_spi.fst")
  
  if(!file.exists(outfile)){
    
    cat(paste0('Processing district: ',districts[i],'\n'))
    
    infile <- flt_clm_subunits2(iso = iso, country = country, district = districts[i])
    tryCatch(expr={
      calc_indices(climate = infile,
                   soil    = soilfl,
                   seasons = seasons,
                   subset  = F,
                   ncores  = 15,
                   outfile = outfile,
                   spi_out = spi_out)
    },
    error=function(e){
      cat(paste0("Modeling process failed in district: ",districts[i],"\n"))
      return("Done\n")
    })
    
    cat(paste0('District: ',districts[i],' finished successfully\n'))
    
  } else {
    cat(paste0('Seasonal indices are already calculated for district: ',districts[i],'\n'))
  }
  
}

if(!file.exists(paste0(root,"/7.Results/",country,"/past/",iso,"_indices.fst"))){
  indices <- list.files(path = paste0(root,"/7.Results/",country,"/past"), pattern = "[0-9]_indices.fst", full.names = T) %>%
    purrr::map(.f = function(f){
      df <- f %>%
        tidyft::parse_fst() %>%
        base::as.data.frame()
      return(df)
    }) %>%
    dplyr::bind_rows()
  tidyft::export_fst(indices, path = paste0(root,"/7.Results/",country,"/past/",iso,"_indices.fst"))
}

if(!file.exists(paste0(root,"/7.Results/",country,"/past/",iso,"_spi.fst"))){
  spis <- list.files(path = paste0(root,"/7.Results/",country,"/past"), pattern = "[0-9]_spi.fst", full.names = T) %>%
    purrr::map(.f = function(f){
      df <- f %>%
        tidyft::parse_fst() %>%
        base::as.data.frame()
      return(df)
    }) %>%
    dplyr::bind_rows()
  tidyft::export_fst(spis, path = paste0(root,"/7.Results/",country,"/past/",iso,"_spi.fst"))
}

# How much area per municipality is on average subject to ‘Major droughts’ (SPI < -1.5)
infile  <- paste0(root,"/7.Results/",country,"/past/",iso,"_spi.fst")
outfile <- paste0(root,'/7.Results/',country,'/past/',iso,'_spi_drought.fst')
calc_spi_drought(spi_data = infile,
                 output   = outfile,
                 country  = country,
                 iso      = iso,
                 seasons  = seasons)

# Calc agro-climatic indices (future)
models  <- c('ACCESS-ESM1-5', 'EC-Earth3-Veg','INM-CM5-0','MPI-ESM1-2-HR','MRI-ESM2-0')
periods <- c('2021-2040','2041-2060')
for(m in models){
  for(p in periods){
    
    districts <- list.files(path = paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_zones'), pattern = '.shp$', full.names = F) %>% gsub('.shp','',.) %>% gtools::mixedsort()
    for(i in 1:length(districts)){
      
      soilfl  <- paste0(root,"/1.Data/soil/",iso,"/soilcp_data.fst")
      outfile <- paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",districts[i],"_indices.fst")
      spi_out <- paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",districts[i],"_spi.fst")
      
      if(!file.exists(outfile)){
        
        cat(paste0('Processing district: ',districts[i],'\n'))
        
        infile <- flt_clm_subunits3(iso = iso, country = country, district = districts[i], model = m, period = p)
        tryCatch(expr={
          calc_indices(climate = infile,
                       soil    = soilfl,
                       seasons = seasons,
                       subset  = F,
                       ncores  = 15,
                       outfile = outfile,
                       spi_out = spi_out)
        },
        error=function(e){
          cat(paste0("Modeling process failed in district: ",districts[i],"\n"))
          return("Done\n")
        })
        
        cat(paste0('District: ',districts[i],' finished successfully\n'))
        
      } else {
        cat(paste0('Seasonal indices are already calculated for district: ',districts[i],'\n'))
      }
      
    }
    
    if(!file.exists(paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_indices.fst"))){
      indices <- list.files(path = paste0(root,"/7.Results/",country,"/future/",m,"/",p), pattern = "[0-9]_indices.fst", full.names = T) %>%
        purrr::map(.f = function(f){
          df <- f %>%
            tidyft::parse_fst() %>%
            base::as.data.frame()
          return(df)
        }) %>%
        dplyr::bind_rows()
      tidyft::export_fst(indices, path = paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_indices.fst"))
    }
    
    if(!file.exists(paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_spi.fst"))){
      spis <- list.files(path = paste0(root,"/7.Results/",country,"/future/",m,"/",p), pattern = "[0-9]_spi.fst", full.names = T) %>%
        purrr::map(.f = function(f){
          df <- f %>%
            tidyft::parse_fst() %>%
            base::as.data.frame()
          return(df)
        }) %>%
        dplyr::bind_rows()
      tidyft::export_fst(spis, path = paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_spi.fst"))
    }
    
    infile  <- paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_spi.fst")
    outfile <- paste0(root,"/7.Results/",country,"/future/",m,"/",p,"/",iso,"_spi_drought.fst")
    calc_spi_drought(spi_data = infile,
                     output   = outfile,
                     country  = country,
                     iso      = iso,
                     seasons  = seasons)
  }
}

## Graphs

# 1. maps
# ISO3 = iso3 = iso
map_graphs(iso3 = iso, country = country, seasons = seasons)

# 2. Elevation map
Elv_map(iso3 = iso, country = country)

# 3. Time series plots
time_series_plot(country = country, iso = iso, seasons = seasons)

# 4. Time series plots by zone 
time_series_region(country = country, iso = iso, seasons = seasons)

# 5. Climatology
climatology_plot(country = country, iso = iso, output = glue::glue('{root}/7.Results/{country}/results/climatology.png'))


# 7. Summary index. 
Other_parameters(country = country, iso3 = iso)

# Seasonal Run. 
data_cons <- read_data_seasons(country = country, iso3 = iso)

index <- tibble(Zone = c('all', regions_all$region) ) %>% 
  dplyr::mutate(index = purrr::map(.x = Zone, .f = function(x){
    indx <- list()
    for(i in 1:length(seasons)){
      indx[[i]] <- summary_index(Zone = x, data_init = data_cons, Period = seasons[i])
    }
    
    indx <- dplyr::bind_rows(indx)
  })) %>% 
  tidyr::unnest() %>% 
  dplyr::select(-cod_name )

write_csv(x = index, file = glue::glue('//dapadfs/workspace_cluster_14/WFP_ClimateRiskPr/7.Results/{country}/sesons_ind.csv') )

# 6. PPT slides
# Run it local. 