# Author: Ani Ghosh, Robert J. Hijmans 
# Date :  May 2020
# Version 0.1
# Licence GPL v3

# Ecocrop single run
runEcocropSingle <- function(i, eco, tmin, tavg, prec, rainfed, clims, outdir, res, iso, ...){
  
  if(!missing(iso)){
    res <- file.path(res, iso)
  }
  
  if(!rainfed){
    clims <- file.path(clims, "temp_only")
  }
  
  outdir <- file.path(outdir, clims, res)
  dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
  
  if (ncol(eco) == 2){
    cropname <- eco[i,2]
    ofile <- paste0("fao_",gsub("[^[:alnum:]]", "_", cropname),"_",eco[i,1], ".tif")
    cropParams <- dismo::getCrop(cropname)
  } else {
    cropname <- eco[i,1]
    if(eco[i,2] == "FAO"){
      cropParams <- getCrop(eco[i,1])
      ofile <- paste0("fao_",gsub("[^[:alnum:]]", "_", cropname), ".tif")
    } else {
      cropParams <- getParsDismo(eco[i,])
      ofile <- paste0("ccafs_",gsub("[^[:alnum:]]", "_", cropname), ".tif")
    }
  }
  ofilename <- file.path(outdir, ofile)
  
  # save input parameter file used
  saveRDS(cropParams, gsub(".tif", ".rds", ofilename))
  
  if(!missing(iso)){
    v <- raster::getData("GADM", country=iso, level=0, path=outdir)
    tmin <- crop(tmin, v)
    tavg <- crop(tavg, v)
    prec <- crop(prec, v)
  }
  
  if(!file.exists(ofilename)){
    
    cat("creating", ofilename, "\n")
    
    pred <- ecocropMod(cropParams, tmin, tavg, prec, rainfed, filename = ofilename, ...)
    # test if the write is successful?
    # writeRaster(pred, ofilename)
    return(ofilename)
  } else {
    cat("already exists ", ofilename, "\n")
  }
  
  flush.console()
}


runEcocropAll <- function(i, runs, ff, eco, outdir, res, rainfed, iso, ...){
  # climate model combination
  k <- runs[i,]
  clims <- paste0(k$gcm, "_", k$rcp, "_", k$years, "s")
  cat("running ", clims, "\n")
  
  f <- grep(clims, ff, value = TRUE)
  f <- gsub(".zip", "", f)
  
  fta <- file.path(grep("tmean", f, value = TRUE), "tmean.tif")
  ftm <- file.path(grep("tmin", f, value = TRUE), "tmin.tif")
  fpr <- file.path(grep("prec", f, value = TRUE), "prec.tif")
  
  tavg <- stack(fta)
  tmin <- stack(ftm)
  prec <- stack(fpr)
  
  lapply(1:nrow(eco), runEcocropSingle, eco, tmin, tavg, prec, rainfed , clims, outdir, res, iso,...)  
  
  cat("completed ", clims, "\n")
  flush.console()
}


