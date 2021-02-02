# CMIP6 download
# install the most recent package
# remotes::install_github("ideas-lab-nus/epwshiftr")

# downloader function
getDataCMIP6 <- function(i, idx, downdir, silent=FALSE, overwrite=FALSE){
  d <- idx[i,]
  # print something
  if (silent) print(d$file_url); flush.console()
  
  # specify where to save
  fstr <- strsplit(d$file_url, "/CMIP6/|/cmip6/")[[1]][2]
  flocal <- file.path(downdir,"CMIP6",fstr)
  d$localfile <- flocal
  
  # where to save
  dir.create(dirname(flocal), FALSE, TRUE)
  
  # should we download?
  if (!file.exists(flocal)|overwrite){
    # try downloading
    dattempt <- try(download.file(d$file_url, flocal, mode = "wb", quiet=silent))
    
    # keep log
    if(inherits(dattempt, "try-error")){
      d$download_status <- "FAIL"
      d$localfilechek <- "FAIL"
    } else {
      d$download_status <- "PASS"
      if (d$file_size == file.size(flocal)){
        d$localfilechek <- "PASS"
      } else {
        d$localfilechek <- "FAIL"
      }
    }
  }
  return(d)
}

##########################################################################################
# which files to download
ifile <- "data/cmip6_index.csv"

if(!file.exists(ifile)){
  library(epwshiftr)
  # set directory to store files
  options(epwshiftr.dir = tempdir())
  options(epwshiftr.verbose = TRUE)
  
  # variables to download
  vars <- c("pr","tas","tasmax","tasmin")
  models <- c("BCC-CSM2-MR","GFDL-ESM4","INM-CM5-0","MPI-ESM1-2-HR","MRI-ESM2-0")
  
  # future scenario
  fidx <- init_cmip6_index(
    activity = "ScenarioMIP",
    experiment = "ssp585",
    frequency = "day",
    variable = vars,
    source = models
  )
  # historical  
  hidx <- init_cmip6_index(
    activity = "CMIP",
    variable = vars,
    frequency = "day",
    experiment = "historical",
    source = models,
  )
  # merge observations
  idx <- rbind(hidx, fidx)
  write.csv(idx, ifile, row.names = FALSE)
} else {
  idx <- read.csv(ifile, stringsAsFactors = FALSE)
}

##########################################################################################
# change the data directory as needed
downdir <- "\\dapadfs.cgiarad.org\workspace_cluster_13\WFP_ClimateRiskPr\\data"

# download files
dwl <- lapply(1:nrow(idx), getDataCMIP6, idx, downdir, silent=FALSE, overwrite=FALSE)

# save download status
dd <- do.call(dwl, rbind)
write.csv(dd, paste0("data/cmip6_download_status_", Sys.Date(), "_", Sys.info()[["nodename"]],".csv"), row.names = FALSE) 