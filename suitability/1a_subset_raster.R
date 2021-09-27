library(raster)

createSubsetRaster <- function(crp, dd, params, outdir, res){
  d <- dd[dd$crops==crp, ]
  isol <- d$ISO
  
  # get name of the crops
  # change the name of crop to match filename
  cropname <- gsub("[^[:alnum:]]", "_", crp)
  # add ccafs, fao
  # search files
  if (crp %in% c("maize_all", "sorghum_all")){
    ifile <- paste0(gsub("[^[:alnum:]]", "_", cropname))
  } else {
    ifile <- paste0(params,"_",gsub("[^[:alnum:]]", "_", cropname))
  }
  
  curfiles <- list.files(file.path(outdir, "worldclim", res), pattern = ifile, full.names = TRUE)
  futfiles <- list.files(file.path(outdir, "future", res), pattern = ifile, full.names = TRUE)
  
  curfiles <- grep(".tif", curfiles, value = TRUE)
  futfiles <- grep(".tif", futfiles, value = TRUE)
  
  # ccafs
  if((length(curfiles)+length(futfiles))==5){
    r1 <- stack(curfiles)
    r2 <- stack(futfiles)
    rr <- stack(r1,r2)
    names(rr) <- c(paste0("current_", cropname), names(r2))
    # parallel::mclapply(isol, cropRas, ifile, rr, outdir, res, mc.preschedule = FALSE, mc.cores = length(isol))
    lapply(isol, cropRas, ifile, rr, outdir, res)
    } else {
    cat(params, " for", cropname, "not available", "\n")
  }
  
  cat("finished ", cropname, "\n\n")
  flush.console()
}

cropRas <- function(iso, ifile, rr, outdir, res){
  
  cat("prcoessing ",iso, ifile, "\n")
  
  # create directory to save results and get the boundary for the country
  fdir <- file.path(outdir, "result", res, iso)
  dir.create(fdir, FALSE, TRUE)
  # output filename
  ofilename <- file.path(fdir, paste0(iso, "_", ifile))
  
  cat("processing", ofilename, "\n")
  if(!file.exists(ofilename)){
    # get vector data
    v <- getData("GADM", country = iso, level = 0, path = fdir)
    nm <- names(rr)
    # crop, mask, save
    rr <- crop(rr, v)
    rr <- mask(rr, v, paste0(ofilename, ".tif"), overwrite = TRUE)
    saveRDS(nm, paste0(ofilename, "_stackname.rds"))
  }
}


#############################################################################################################
# country-crop combination
eco <- readxl::read_excel("suitability/ecocrop_runs.xlsx", sheet = 4)

# create a database of crop-country
# read and process every crop raster for once, and crop all the countries that has the crops
dd <- list()
for (i in 1:nrow(eco)){
  ecos <- eco[i,]
  # cleanup crop name
  crops <- trimws(unlist(strsplit(ecos$cropname, ",")))

  # remove poultry, milk
  crops <- crops[!crops %in% c("poultry", "ship/goat", "fish", "cattle", "salt")]
  iso <- ecos$iso3
  dd[[i]] <- data.frame(ISO=iso, crops = crops, stringsAsFactors = FALSE)
}

dd <- do.call(rbind, dd)
dd <- unique(dd[,1:2])
# 
# # list of unique crops
ucrp <- unique(dd$crops)

# final call
# input
res <- "2_5min"
outdir <- "G:\\My Drive\\work\\ciat\\ecocrop"

lapply(ucrp, createSubsetRaster, dd, "ccafs", outdir, res)
lapply(ucrp, createSubsetRaster, dd, "fao", outdir, res)



#####################################################################################################
library(terra)

meanRas <- function(grp, fcn){
  f <- grep(paste0("rcp",grp), fcn, value = TRUE)
  r <- rast(f)
  r <- mean(r, na.rm = TRUE)
  return(r)
}

meanRasAll <- function(crp, iso, rr, fdir){
  cat("Processing ", crp, "for", iso, "\n")
  fcn <- grep(crp, rr, value = TRUE)
  # sometimes not all crops are present in both scenario (need to fix)
  if(length(fcn) > 10){
    grps <- c("4_5_2030", "4_5_2050", "8_5_2030", "8_5_2050")
    rout <- lapply(grps, meanRas, fcn)
    rout <- rast(rout)
    wc <- rast(grep("worldclim", fcn, value = TRUE))
    rout <- c(wc, rout)
    bnames <- paste0(crp, "_", c("current", grps))
    fout <- file.path(fdir, paste0(iso,"_",crp,".tif"))
    bout <- file.path(fdir, paste0(iso,"_",crp,"_stackname.rds"))
    
    writeRaster(rout, fout, overwrite = TRUE)
    saveRDS(bnames, bout)
  } else {
    cat(cn, " does not have all the predictions \n")
  }
}

# for countries processed individually, but results need to be combined now
dir <- "C:/Users/anibi/Documents/ciat/giz"
datadir <- file.path(dir, "potato/ecocrop")

# input files
ra <- list.files(datadir, pattern = ".tif$",recursive = TRUE, full.names = TRUE)
ra <- grep("temp_only", ra, value=TRUE)

# list of countries  
isol <- c("CMR","IND", "MLI", "NGA", "TUN")
crps <- c("Potato_Drought_Tolerant", "Potato_Early_Maturing", "Potato_Heat_Tolerant", "Potato_Typical")

for (iso in isol){
  fdir <- file.path(datadir, "future", iso)
  dir.create(fdir, FALSE, TRUE)
  rr <- grep(iso, ra, value = TRUE)
  lapply(crps, meanRasAll, iso, rr, fdir)
}


####################################################################################
# Mask cocoa suitability by aridity index
library(raster)
dir <- "C:/Users/anibi/Documents/ciat/giz"
ref <- stack(file.path(dir, "aridity_thornthwaite/aridity_thornthwaite_hist.tif"))

ff <- list.files(path=file.path(dir, "iso"), pattern="*Cacao_unmasked_by_aridity*.tif$", 
                 recursive=TRUE, full.names=TRUE)

p <- stack(ff)
rc <- crop(ref, p)
rc[rc >= 75] <-NA
pm <- mask(p, rc, filename=gsub("_unmasked_by_aridity", "", ff))
