options(warn = -1, scipen = 999)
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse,tidyft,terra))

OSys <- Sys.info()[1]
root <<- switch(OSys,
                'Linux'   = '/dapadfs/workspace_cluster_13/WFP_ClimateRiskPr',
                'Windows' = '//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr')
rm(OSys)

iso <- 'SOM'
model <- 'ACCESS-ESM1-5'
period <- '2041-2060'

out <- paste0(root,'/1.Data/future_data/',model,'/SOM/bias_corrected/',period,'/',iso,'.fst')

if(!file.exists(out)){
  # List and load all list of chunks
  bc_data <- list.files(paste0(root,'/1.Data/future_data/',model,'/',iso,'/bias_corrected/',period), pattern = '_chunk_', full.names = T) %>%
    purrr::map(.f = function(f){
      df <- f %>%
        tidyft::parse_fst(.) %>%
        base::as.data.frame(.)
      return(df)
    }) %>%
    dplyr::bind_rows()
  # Save big table
  tidyft::export_fst(x = bc_data, path = out)
}
