# download ERA5 agro-met indicators
# to run in GCP instance
# CDS dataset description
# https://cds.climate.copernicus.eu/cdsapp#!/dataset/sis-agrometeorological-indicators?tab=overview

# Has a number of dependencies, working on alternatives
if("ecmwfr" %in% rownames(installed.packages()) == FALSE) {
  install.packages("ecmwfr", dependencies = TRUE)}

library("ecmwfr")

# save key for CDS
# use the credentials I shared
wf_set_key(user = uid,
           key = key,
           service = "cds")


getERA5 <- function(i, qq, year, month, day, datadir, uid){
  q <- qq[i,]
  format <- "zip"
  ofile <- paste0(datadir, "/", paste(q$variable, q$statistics, year, month, day, sep = "-"), ".",format)
  
  if(!file.exists(ofile)){
    cat("Downloading", q[!is.na(q)], "for", year, month, "\n"); flush.console();
    request <- list("dataset_short_name" = "sis-agrometeorological-indicators",
                    "variable" = q$variable,
                    "statistics" = q$statistics,
                    "year" = year,
                    "month" = month,
                    "day" = day,
                    "area" = "90/-180/-90/179.9", # download global #c(ymax,xmin,ymin,xmax)? 
                    "time" = q$time,
                    "format" = format,
                    "target" = ofile)
    
    request <- Filter(Negate(anyNA), request)
    
    file <- wf_request(user     = uid ,   # user ID (for authentification)
                       request  = request,  # the request
                       transfer = TRUE,     # download the file
                       path     = datadir)  
  }
   
  return(NULL)
}


########################################################################################################
# TODO: change data directory
datadir <- "~/data/input/climate/era5/agmet"
datadir <- "/cluster01/workspace/ONECGIAR/Data/climate/era5/sis-agromet/zip"
dir.create(datadir, FALSE, TRUE)

# combinations to download
qq <- data.frame(variable = c("precipitation_flux","solar_radiation_flux",rep("2m_temperature",3),
                              "10m_wind_speed", "2m_relative_humidity"),
                 statistics = c(NA, NA, "24_hour_maximum", "24_hour_mean", "24_hour_minimum",
                                "24_hour_mean", NA),
                 time = c(NA,NA,NA,NA,NA,NA, "12_00"))

# temporal range
years <- as.character(1979:2020)
months <- c(paste0("0", 1:9), 10:12)

uid <- "76741"
# all download
for (i in 1:nrow(qq)){
  for (year in years){
    for (month in months){
      sdate <- paste0(year, "-", month, "-01")
      ndays <- lubridate::days_in_month(sdate)
      days <- sprintf("%02d", 1:ndays)
      for (day in days){
        try(getERA5(i, qq, year, month, day, datadir, uid))
      }
    }
  }
}


# to deleted keyring use keyring package
# keyring::keyring_list()$keyring
# keyring::keyring_delete(keyring = "ecmwfr")
# unzip example
# unzip(file.path(datadir, ofile), exdir = datadir)

############################################################################################
# unsuccessful attempts
# IMP: install csdapi
# Linux: https://cds.climate.copernicus.eu/api-how-to#install-the-cds-api-key
# Windows: https://confluence.ecmwf.int/display/CKB/How+to+install+and+use+CDS+API+on+Windows

# install.packages("reticulate")
# library(reticulate)
# 
# #install the python ECMWF API
# # py_install("cdsapi")
# 
# #import python library cdsapi
# cdsapi <- import('cdsapi')
# 
# c <- cdsapi$Client()
# 
# dataset <- 'sis-agrometeorological-indicators'
# request <- "{
#     'format': 'zip',
#     'variable': '2m_temperature',
#     'statistics': '24_hour_maximum',
#     'year': '1979',
#     'month': '01',
#   }"
# 
# result <- c$retrieve(dataset, request, 'download.zip')

# Previous attempt to get ERA5 data from Google Earth Engine
# Requirement: 
# use daily data as it is, convert hourly data to daily data
# export for each country to Google Cloud Storage (GCS)/ Google Drive
# copy from cloud to \\dapadfs.cgiarad.org\workspace_cluster_13\WFP_ClimateRiskPr (???)

# Earth Engine link to export ERA5 data
# https://code.earthengine.google.com/a48864fc4198e1e8a79a70750f0f1525

# add clean and load