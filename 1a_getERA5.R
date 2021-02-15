# IMP: install csdapi
# Linux: https://cds.climate.copernicus.eu/api-how-to#install-the-cds-api-key
# Windows: https://confluence.ecmwf.int/display/CKB/How+to+install+and+use+CDS+API+on+Windows

# install.packages("reticulate")
library(reticulate)

#install the python ECMWF API
# py_install("cdsapi")

#import python library cdsapi
cdsapi <- import('cdsapi')

c <- cdsapi$Client()

dataset <- 'sis-agrometeorological-indicators'
request <- "{
    'format': 'zip',
    'variable': '2m_temperature',
    'statistics': '24_hour_maximum',
    'year': '1979',
    'month': '01',
  }"

result <- c$retrieve(dataset, request, 'download.zip')


# Previous attempt to get ERA5 data from Google Earth Engine
# Requirement: 
# use daily data as it is, convert hourly data to daily data
# export for each country to Google Cloud Storage (GCS)/ Google Drive
# copy from cloud to \\dapadfs.cgiarad.org\workspace_cluster_13\WFP_ClimateRiskPr (???)

# Earth Engine link to export ERA5 data
# https://code.earthengine.google.com/a48864fc4198e1e8a79a70750f0f1525

# add clean and load


# Load required libraries
require(httr)
#' cds_retrieve
#'
#' Function to retrieve data from the Copernicus Climate Change Service (C3S) Climate Data Store
#'
#' @param dataset Dataset to extract data from.
#' @param request JSON string containing request parameters.
#' @param target  Filename to save data to.
#' @param request_id Previous request ID returned by function.
#' @return List containing request ID and name of data file downloaded, NA returned if timeout
#' @note API Key read from ~/.cdsapirc
#' @examples
#' dataset <- "reanalysis-era5-single-levels"
#' request <- "{
#'             'variable': 'mean_sea_level_pressure',
#'             'grid': ['0.1', '0.1'], 'product_type': 'reanalysis',
#'             'year': '2010', 'month': '08', 'day': '15', 'time': '12:00',
#'             'area': '45.0/-15.0/70.0/25.0',
#'             'format': 'netcdf'
#'             }"
#' result <- cds_retrieve( dataset, request)
#' # If timeout the following can be used to try again without generating new request
#' result <- cds_retrieve( dataset, request, request_id = result$request_id)
#'
#' @export
cds_retrieve <- function(dataset, request, target = '' , request_id = NA){
  # load connection details
  cdsConn <- readLines('~/.cdsapirc')
  # extract CDS URL
  cdsURL  <- cdsConn[ grep('url', cdsConn)   ]
  cdsURL  <- gsub( 'url: ', '', cdsURL)
  # extract CDS authentication details
  cdsKey  <- cdsConn[ grep('key', cdsConn) ]
  cdsUID  <- unlist( strsplit( cdsKey, ':') )[2]
  cdsUID  <- gsub( "\\s+","",cdsUID)
  cdsPWD  <- unlist( strsplit( cdsKey, ':') )[3]
  if (is.na( request_id) ){
    # Now we should be ready to connect and retrieve the data
    # first strip any extra whitespace from request string
    req <- gsub('\\s+',' ',request)
    # Now post request to service
    dataURL <- paste0( cdsURL, '/resources/', dataset)
    response <- POST(  dataURL, body = req, encode = 'json', authenticate( cdsUID, cdsPWD  )  )
    # Check for error
    if( http_error(response) ){
      print( content( response ) )
      stop()
    }
    request_id <- content(response)$request_id
  }else{
    source <- paste0( cdsURL, '/tasks/', request_id )
    response <- GET( source , authenticate(cdsUID, cdsPWD) )
  }
  # check if ready, loop until timeout if not
  if( content(response)$state != 'completed' ){
    waiting <- TRUE
    sleeptime = 1
    while( waiting ){
      # Check status
      source <- paste0( cdsURL, '/tasks/', request_id )
      response <- GET( source , authenticate(cdsUID, cdsPWD) )
      print( content( response ) )
      if( content( response )$state == 'completed' ){
        waiting <- FALSE
      }else if( content( response )$state %in% c('queued','running') ){
        Sys.sleep( sleeptime )
        sleeptime <- sleeptime * 1.5
      }else{
        waiting <- FALSE
      }
      if( sleeptime > 60)waiting <- FALSE
    }
  }
  # either time out or data ready
  if( content(response)$state == 'completed' ){
    # download the data
    if (target == ''){
      outfile = tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".nc")
    }else{
      outfile = target
    }
    GET( url = content(response)$location , write_disk(outfile, overwrite = TRUE) )
  }else{
    print( "Timeout ..." )
    outfile = NA
  }
  return_value = list( request_id = request_id, data_file = outfile)
  return( return_value )
}
