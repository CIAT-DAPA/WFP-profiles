source("archive/999_1b_search_CMIP6_functions.R")
options(timeout=3600)
vars <- c("pr","tas","tasmax","tasmin")
# models <- c("BCC-CSM2-MR","CESM2","INM-CM5-0","MPI-ESM1-2-HR","MRI-ESM2-0")
models <- c("ACCESS-ESM1-5","EC-Earth3-Veg","INM-CM5-0","MPI-ESM1-2-HR","MRI-ESM2-0")

# models <- c("ACCESS-CM2", "ACCESS-ESM1-5", "BCC-CSM2-MR", "CanESM5", "CIESM", "CMCC-ESM2",
#             "CNRM-CM6-1", "CNRM-CM6-1-HR", "CNRM-ESM2-1", "FGOALS-g3", "FIO-ESM-2-0", "GISS-E2-1-G", "GFDL-ESM4",
#             "HadGEM3-GC31-LL", "INM-CM4-8", "INM-CM5-0", "IPSL-CM6A-LR", "MIROC-ES2L", "MIROC6", "MRI-ESM2-0",
#             "MPI-ESM1-2-LR", "NESM3")

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
                               member_id = "r1i1p1f1",
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

# datadir <- "/cluster01/workspace/common/climate/cmip6/daily"
# dir.create(datadir, F, T)
# data.table::fwrite(dd, file.path(datadir, paste0("cmip6_filter_index_", Sys.Date(), ".csv")), row.names = FALSE)

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
  
  file_damage <- FALSE
  
  tryCatch({ result <- raster::stack(flocal) }, error = function(e) {file_damage <<- TRUE})
  # expand for other kind type of files
  
  if(file_damage){
    cat("deleting", basename(flocal), "\n")
    unlink(flocal)
    flush.console()
  }else{
    cat("keeping", basename(flocal), "\n") 
  }
  
  return(NULL)
}

##########################################################################################
# change the data directory as needed
downdir <- "/cluster01/workspace/common/climate/cmip6/daily"
f <- list.files("data", pattern = "cmip6_filter_index_", full.names = TRUE)
j <- which.max(file.info(f)$ctime)
idx <- read.csv(f[j], stringsAsFactors = FALSE)
idx <- idx[idx$file_start_date < "2100-12-31" & idx$file_end_date > "1950-01-01",]
# downdir <- file.path(datadir, "daily")
# f <- list.files(datadir, ".csv$", full.names = TRUE)
# idx <- read.csv(f, stringsAsFactors = FALSE)


downloadParallel <- FALSE

if (downloadParallel){
  library(future.apply)
  plan(multiprocess, workers = 12)
  future_lapply(1:nrow(idx), getDataCMIP6, idx, downdir, silent=FALSE, future.seed = TRUE)
} else {
  # download files
  lapply(1:nrow(idx), getDataCMIP6, idx, downdir, silent=FALSE)
}


deleteCorruptDownloads <- function(f){
  
  file_damage <- FALSE
  
  tryCatch({ result <- raster::stack(f) }, error = function(e) {file_damage <<- TRUE})
  # expand for other kind type of files
  
  if(file_damage){
    cat("deleting", basename(f), "\n")
    unlink(f)
    flush.console()
  }else{
    cat("keeping", basename(f), "\n") 
  }
}
ff <- list.files(downdir, pattern = ".nc",recursive = TRUE, full.names = TRUE)
lapply(ff, deleteCorruptDownloads)


# check for file size for downloaded file 
# k <- lapply(1:nrow(idx), checkDownloadStatus, dd, downdir)
# k <- data.table::rbindlist(k, fill = TRUE)
# table(k$localfilecheck)

# move/delete files not in the updated list 
ff <- list.files(downdir, pattern = ".nc$", full.names = TRUE)
# f <- ff[!ff %in% path.expand(k$localfile)]
# f <- grep("r1i1p1f1", ff, invert = TRUE, value = TRUE)
f <- unique(grep(paste(models,collapse="|"), ff, value=TRUE, invert = TRUE))

backup <- gsub("climate","backup/climate",downdir)
dir.create(backup, recursive = T, showWarnings = F)
file.copy(f, backup)
unlink(f)
# model distribution
m <- sapply(strsplit(dhists$id, ".historical"), "[[", 1)
table(gsub("CMIP6.CMIP.", "", m))



dd <- read.csv("C:/Users/anibi/Downloads/cmip6_filter_index_2021-10-05.csv")
x <- idx[grep("INM-CM5-0", idx$id),]
View(x)
