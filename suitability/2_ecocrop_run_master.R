# Author: Ani Ghosh 
# Date :  May 2020
# Version 0.1
# Licence GPL v3


# run ecocrop
source("suitability/0_ecocrop_functions.R")
source("suitability/1_ecocrop_multiple.R")

####################################################################################
# setup run
library(raster)
library(dismo)

# for each model/rcp/year run all crops (original FAO and modified)
datadir <- "data/climate"
pdir <- "data"
outdir <- file.path(pdir,"out/ecocrop")

# option to read both FAO and modified parameters
eco1 <- readxl::read_excel("suitability/ecocrop_runs.xlsx", sheet = 1)

# modified parameters
eco2 <- readxl::read_excel("suitability/ecocrop_runs.xlsx", sheet = 2)
names(eco2) <-  c("crop","crop_calendar","suitability","suitability_seasons","Gmin","Gmax","GSL_months", 
                  "Tkill","Tmin","Topmin","Topmax", "Tmax", "Rmin","Ropmin","Ropmax","Rmax","Comments") 
eco2 <- dplyr::filter(eco2, suitability == "EcoCrop")

# if need to run a specific set of crops
# eco <- eco1[eco1$crop %in% "list_of_crops_as_present_in_the_excel",]

# resolution
k <- 2
pres <- c(0.5, 2.5, 5, 10)
res <- pres[2]

if (res==2.5) { 
  res <- '2_5min' 
} else if (res == 0.5) {
  res <- "30s"
} else {
  res <- paste(res, 'min', sep='')
}

##############################################################################################################

outdir <- "work/out/ecocrop"

#############################################################################################################################################################
# run on worldclim
vars <- c("tmean","tmin","prec")
wc <- lapply(vars, function(var)getData('worldclim', var=var, res=pres[k], path = datadir))
tavg <- wc[[1]]
tmin <- wc[[2]]
prec <- wc[[3]]  

eco <- eco2

# run one country at a time
isol <- "TCD"

for (iso in isol){
  cores <- 4
  parallel::mclapply(1:nrow(eco), runEcocropSingle, eco, tmin, tavg, prec, 
                     rainfed = FALSE, "worldclim", outdir, res, iso,
                     mc.preschedule = FALSE, mc.cores = cores) 
}

#############################################################################################################################################################
# run for future climate
# location of climate data
cmipdir <- file.path(datadir, "cmip5", res)

# list all files
ff <- list.files(cmipdir, pattern = "*.zip", full.names = TRUE)

# we need to run the model for every gcm/rcp/year combination
gcm <- tolower(c("MOHC_HADGEM2_ES", "CESM1_CAM5", "GFDL_CM3", "MPI_ESM_LR", "MIROC_MIROC5"))
rcp <- paste0("rcp", c("4_5", "8_5"))
# years <- c(2030, 2050, 2070, 2080)
years <- c(2030, 2050)

# combination of run
runs <- expand.grid(rcp = rcp,  years = years, gcm = gcm, stringsAsFactors = FALSE)

# final run
for (iso in isol){
  cores <- 4
  parallel::mclapply(1:nrow(runs), runEcocropAll, runs, ff, eco, outdir, res, rainfed=FALSE, iso=iso,
                     mc.preschedule = FALSE, mc.cores = cores)
  
}