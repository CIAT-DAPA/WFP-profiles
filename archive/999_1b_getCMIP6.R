# CMIP6 download
# to run in GCP instance
# install the most recent version of epwshiftr
if("epwshiftr" %in% rownames(installed.packages()) == FALSE) {
  remotes::install_github("ideas-lab-nus/epwshiftr")}

# to avoid download time out?
options(timeout=3600)

# downloader function
getDataCMIP6 <- function(i, idx, downdir, silent=FALSE, overwrite=FALSE){
  d <- idx[i,]
  # print something
  if (silent) print(d$file_url); flush.console()
  
  # specify where to save
  # fstr <- strsplit(d$file_url, "/CMIP6/|/cmip6/")[[1]][2]
  flocal <- file.path(downdir, basename(d$file_url))
  d$localfile <- flocal
  
  # where to save
  dir.create(dirname(flocal), FALSE, TRUE)
  
  # should we download?
  if (!file.exists(flocal)|overwrite){
    # try downloading
    dattempt <- try(download.file(d$file_url, flocal, mode = "wb", quiet=silent))
  }
  return(NULL)
}

##########################################################################################
checkDownloadStatus <- function(i, idx, downdir){
  d <- idx[i,]
  
  d$download_status <- "FAIL"
  d$localfilechek <- "FAIL"
  
  # localfile exists?
  # fstr <- strsplit(d$file_url, "/CMIP6/|/cmip6/|/cmip6_data/")[[1]][2]
  flocal <- file.path(downdir, basename(d$file_url))
  d$localfile <- flocal
  
  localfilecheck <- d$file_size == file.size(flocal)
  
  if(file.exists(flocal) & localfilecheck){
    d$download_status <- "PASS"
    d$localfilechek <- "PASS"
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
downdir <- "~/data/input/climate/CMIP6/daily"

# download files
lapply(1:nrow(idx), getDataCMIP6, idx, downdir, silent=FALSE, overwrite=FALSE)
# library(future.apply)
# plan(multiprocess, workers = 12)
# future_lapply(1:nrow(idx), getDataCMIP6, idx, downdir, silent=FALSE, overwrite=FALSE, future.seed = TRUE)


# files that failed download
dws <- lapply(1:nrow(idx), checkDownloadStatus, idx, downdir)
ds <- data.table::rbindlist(dws, fill = TRUE)
sidx <- ds[ds$download_status == "FAIL"| ds$localfilechek == "FAIL",]

# keep trying unless everything works
while(nrow(sidx) > 0){
  lapply(1:nrow(sidx), getDataCMIP6, idx, downdir, silent=FALSE, overwrite=TRUE)
  # library(future.apply)
  # plan(multiprocess, workers = 12)
  # future_lapply(1:nrow(idx), getDataCMIP6, idx, downdir, silent=FALSE, overwrite=FALSE, future.seed = TRUE)
  # check download status and prep for rerun
  dws <- lapply(1:nrow(idx), checkDownloadStatus, idx, downdir)
  ds <- data.table::rbindlist(dws, fill = TRUE)
  sidx <- ds[ds$download_status == "FAIL" | ds$localfilechek == "FAIL",]
  # print(nrow(sidx))
}

# save final log
dws <- lapply(1:nrow(idx), checkDownloadStatus, idx, downdir)
ds <- data.table::rbindlist(dws, fill = TRUE)
write.csv(ds, paste0("data/cmip6_download_status_", Sys.Date(), "_", Sys.info()[["nodename"]],".csv"), row.names = FALSE) 


# copy files out of complex folder structure and keep them in the daily folder
# ff <- list.files("~/data/input/climate/CMIP6/", pattern = ".nc$", full.names = TRUE, recursive = TRUE)
# ff <- grep("daily", ff, invert = TRUE, value = TRUE)
# file.copy(ff, file.path(downdir, "CMIP6", "daily"))
# unlink(ff)
