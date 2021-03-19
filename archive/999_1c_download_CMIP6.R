source("archive/999_1b_search_CMIP6_functions.R")

vars <- c("pr","tas","tasmax","tasmin")
models <- c("BCC-CSM2-MR","CESM2","INM-CM5-0","MPI-ESM1-2-HR","MRI-ESM2-0")

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
                             variable_id = var,
                             source_id = model,
                             mip_era = "CMIP6"))
}

# remove any unsuccessful attempts
dhc <- lapply(dh, function(x){if(inherits(x, "data.table")){return(x)}else{NULL}})

dhist <- data.table::rbindlist(dhc, fill = TRUE)
# list of variant labels
# table(unlist(dhist$member_id))
# subset by member_id that has maximum results
hm <- table(unlist(dhist$member_id))
hmx <- which(hm == max(hm))
# if there are more than 1
hmx <- names(which(hmx == max(hmx)))

dhists <- dhist[dhist$member_id == hmx, ]

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
# list of variant labels
# table(unlist(dfut$member_id))
# subset by member_id that has maximum results
fm <- table(unlist(dfut$member_id))
fmx <- which(fm == max(fm))
# if there are more than 1
fmx <- names(which(fmx == max(fmx)))

dfuts <- dfut[dfut$member_id == fmx, ]


# combine both results
dd <- rbind(dhists, dfuts)
data.table::fwrite(dd, paste0("data/cmip6_filter_index_", Sys.Date(), ".csv"), row.names = FALSE)

############################################################################################
# now download
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
    try(download.file(d$file_url, flocal, mode = "wb", quiet=silent))
  }
  return(NULL)
}

##########################################################################################
# change the data directory as needed
downdir <- "~/data/input/climate/CMIP6/daily"

idx <- read.csv("data/cmip6_filter_index_2021-03-18.csv", stringsAsFactors = FALSE)

downloadParallel <- FALSE

if (downloadParallel){
  library(future.apply)
  plan(multiprocess, workers = 12)
  future_lapply(1:nrow(idx), getDataCMIP6, idx, downdir, silent=FALSE, overwrite=FALSE, future.seed = TRUE)
} else {
  # download files
  lapply(1:nrow(idx), getDataCMIP6, idx, downdir, silent=FALSE, overwrite=FALSE)
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

