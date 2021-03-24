source("archive/999_1b_search_CMIP6_functions.R")

vars <- c("pr","tas","tasmax","tasmin")
# models <- c("BCC-CSM2-MR","CESM2","INM-CM5-0","MPI-ESM1-2-HR","MRI-ESM2-0")
models <- c("AWI-CM-1-1-MR","EC-Earth3-Veg","INM-CM5-0","MPI-ESM1-2-HR","MRI-ESM2-0")


varmod <- expand.grid(vars, models)
names(varmod) <- c("vars", "models")

############################################################################################
# historical
dh <- list()

for (i in 1:nrow(varmod)){
  var <- varmod$vars[i]
  model <- varmod$models[i]
  dh[[i]] <- try(getMetaCMIP6(offset = 0,
                             limit = 10000,
                             activity_id="CMIP",
                             experiment_id = "historical",
                             frequency = "day",
                             member_id = "r1i1p1f1",
                             variable_id = var,
                             source_id = model,
                             mip_era = "CMIP6"))
}

# remove any unsuccessful attempts
dhc <- lapply(dh, function(x){if(inherits(x, "data.table")){return(x)}else{NULL}})
dhist <- data.table::rbindlist(dhc, fill = TRUE)


####################################################################################
# future
df <- list()

for (i in 1:nrow(varmod)){
  var <- varmod$vars[i]
  model <- varmod$models[i]
  df[[i]] <- try(getMetaCMIP6(offset = 0,
                               limit = 10000,
                               activity_id="ScenarioMIP",
                               experiment_id = "ssp585",
                               frequency = "day",
                               variable_id = var,
                               source_id = model,
                               mip_era = "CMIP6"))
}

# remove any unsuccessful attempts
dfc <- lapply(df, function(x){if(inherits(x, "data.table")){return(x)}else{NULL}})
dfut <- data.table::rbindlist(dfc, fill = TRUE)

# combine both results
dd <- rbind(dhist, dfut)
data.table::fwrite(dd, paste0("data/cmip6_filter_index_", Sys.Date(), ".csv"), row.names = FALSE)

############################################################################################
# now download
options(timeout=3600)

# downloader function
getDataCMIP6 <- function(i, idx, downdir, silent=FALSE){
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
  if (!file.exists(flocal)){
    # try downloading
    try(download.file(d$file_url, flocal, mode = "wb", quiet=silent))
  }
  return(NULL)
}

##########################################################################################
# change the data directory as needed
downdir <- "~/data/input/climate/CMIP6/daily"

idx <- read.csv("data/cmip6_filter_index_2021-03-24.csv", stringsAsFactors = FALSE)

downloadParallel <- FALSE

if (downloadParallel){
  library(future.apply)
  plan(multiprocess, workers = 12)
  future_lapply(1:nrow(idx), getDataCMIP6, idx, downdir, silent=FALSE, future.seed = TRUE)
} else {
  # download files
  lapply(1:nrow(idx), getDataCMIP6, idx, downdir, silent=FALSE)
}


# check for file size for downloaded file 
k <- lapply(1:nrow(idx), checkDownloadStatus, dd, downdir)
k <- data.table::rbindlist(k, fill = TRUE)
table(k$localfilecheck)

# move/delete files not in the updated list 
ff <- list.files(downdir, pattern = ".nc$", full.names = TRUE)
f <- ff[!ff %in% path.expand(k$localfile)]
backup <- gsub("climate","backup/climate",downdir)
dir.create(backup, recursive = T, showWarnings = F)
file.copy(f, backup)
unlink(f)
# model distribution
m <- sapply(strsplit(dhists$id, ".historical"), "[[", 1)
table(gsub("CMIP6.CMIP.", "", m))

