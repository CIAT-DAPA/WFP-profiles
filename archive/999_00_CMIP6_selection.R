# get a list of all possible sources from the pangeo data and sort them based on availabilities
library(data.table)
url <- "https://storage.googleapis.com/cmip6/pangeo-cmip6.csv"
cmip6cat <- paste0("/home/anighosh/data/", basename(url))
download.file(url, destfile = cmip6cat, mode = "wb")
adf <- fread(cmip6cat)
unique(adf$source_id)


##########################################################################################
dhist <- adf[adf$experiment_id == "historical" & adf$variable_id %in% vars, ]
dfut <- adf[adf$experiment_id == "ssp585" & adf$variable_id %in% vars & adf$activity_id == "ScenarioMIP", ]

f <- table(dfut$source_id, dfut$member_id)
h <- table(dhist$source_id, dhist$member_id)

cnames <- intersect(colnames(h), colnames(f))
rnames <- intersect(rownames(h), rownames(f))

h <- h[rownames(h) %in% rnames, colnames(h) %in% cnames]
f <- f[rownames(f) %in% rnames, colnames(f) %in% cnames]

h <- data.frame(unclass(h))
f <- data.frame(unclass(f))

# for every source, find non-zero 

write.csv(h, "/home/anighosh/data/historical_models_table.csv")
write.csv(f, "/home/anighosh/data/future_models_table.csv")

# limit oursleves to r == 4
members <- c("r1i1p1f1", "r2i1p1f1", "r3i1p1f1", "r4i1p1f1")
h1 <- h[, colnames(h) %in% members]
f1 <- f[, colnames(f) %in% members]

hh <- data.frame(source_id = row.names(h1), historical = rowSums(h1))
ff <- data.frame(source_id = row.names(f1), future = rowSums(f1))

hf <- merge(hh, ff) 
hf$sum <- hf$historical + hf$future
hf <- hf[with(hf, order(-sum,-historical, -future)),]

# top 20 models
models <- hf$source_id[1:20]

# table with only the top 10/13 models?
# issue
# http://esgf3.dkrz.de/thredds/catalog/esgcet/619/CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp585.r6i1p1f1.day.pr.gn.v20190710.xml#CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp585.r6i1p1f1.day.pr.gn.v20190710|application/xml+thredds|THREDDS
###########################################################################################################
# now check for file availabilities
source("archive/999_1b_search_CMIP6_functions.R")

vars <-  c("pr","tas","tasmax","tasmin")

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
hm <- table(unlist(dhist$source_id),unlist(dhist$member_id))
hm <- data.frame(unclass(hm))
write.csv(hm, "/home/anighosh/data/historical_models_table.csv")


hms <- hm[, colSums(hm) > 2000]
View(hms)
# 
# hm <- table(unlist(dhist$member_id))
# hmx <- which(hm == max(hm))
# # if there are more than 1
# hmx <- names(which(hmx == max(hmx)))
# 
# dhists <- dhist[dhist$member_id == hmx, ]

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

fm <- table(unlist(dfut$source_id),unlist(dfut$member_id))
fm <- data.frame(unclass(fm))
write.csv(fm, "/home/anighosh/data/future_models_table.csv")


fm <- table(unlist(dfut$member_id))
fmx <- which(fm == max(fm))
# if there are more than 1
fmx <- names(which(fmx == max(fmx)))
dfuts <- dfut[dfut$member_id == fmx, ]

fms <- fm[, colSums(fm) > 1000]
View(fms)

hmx <- row.names(hms)[order(hms$r1i1p1f1, decreasing = TRUE)[1:15]]
fmx <- row.names(fms)[order(fms$r1i1p1f1, decreasing = TRUE)[1:15]]

sort(intersect(hmx, fmx))

# # this is from https://cds.climate.copernicus.eu/cdsapp#!/dataset/projections-cmip6?tab=form
# hms <- c("CMCC-CM2-HR4", "CNRM-CM6-1", "CNRM-ESM2-1", "EC-Earth3-AerChem","EC-Earth3-Veg-LR","FGOALS-g3",
#          "HadGEM3-GC31-MM","INM-CM4-8","KACE-1-0-G","MIROC6","MPI-ESM1-2-HR","MRI-ESM2-0","SAM0-UNICON",
#          "UKESM1-0-LL","AWI-ESM-1-1-LR","BCC-ESM1","CanESM5","CMCC-CM2-SR5","CNRM-CM6-1-HR","FGOALS-f3-L",
#          "HadGEM3-GC31-LL","IITM-ESM","INM-CM5-0","MIROC-ES2L","MPI-ESM1-2-LR","NorESM2-MM","TaiESM1")
# 
# fms <- c("AWI-CM-1-1-MR","CNRM-CM6-1","FGOALS-g3","HadGEM3-GC31-MM","INM-CM4-8","KACE-1-0-G","MRI-ESM2-0",
#          "NorESM2-LM","UKESM1-0-LL","CanESM5","CMCC-CM2-SR5","CNRM-CM6-1-HR","HadGEM3-GC31-LL","IITM-ESM",
#          "INM-CM5-0","MIROC-ES2L","MPI-ESM1-2-LR","NorESM2-MM")
# 
# sort(intersect(hms,fms))