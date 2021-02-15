library(terra)
library(data.table)
library(fst)

if (packageVersion("terra") < "1.1.0"){
  warning("terra version should be at least 1.1.0, \ninstall the the most updated version remotes::install_github('rspatial/terra')")
}

# function to get daily data from a single file
getGCMdailyTable <- function(iso, var, model, experiment, gcmdir, ff, sdare, edate, ref){
  cat("Processing", iso, var, model, experiment, "\n" )
  
  # where to save results
  outdir <- file.path(gcmdir, "gcm_0_05deg_lat", iso)
  dir.create(outdir, FALSE, TRUE)
  
  # country/zone boundary
  # TODO: replace this by WFP country directory
  v <- vect(raster::getData("GADM", country = iso, level = 0, path = "data/"))
  e <- ext(v)*1.25
  # crop reference raster
  riso <- crop(ref, e)
  
  # search files
  f <- grep(paste(model,experiment,var,sep = ".*"), ff, value = TRUE)
  
  # GCM raster prep ############################################################
  # which ranges fall within start and end year
  r <- rast(f)
  # manage and convert to epoch time
  tm <- as.POSIXct(r@ptr$time, origin="1970-01-01")
  # which bands are within the wmo baseline
  k <- which(tm >= sdate & tm <= edate)
  # subset those bands
  r <- subset(r, k)
  r <- rotate(r)
  # reproject outline to same coordinate system if needed
  # vp <- project(v, crs(r))
  # ep <- ext(vp)*1.25
  rx <- crop(r, e)
  # corresponding time; for now save as unix, convert to epoch later
  stm <- r@ptr$time
  
  # study area focus ###########################################################
  # resample to reference raster --- most time taking part
  ofile <- file.path(outdir, paste(iso,model,experiment,var,sdate,edate, sep = "_"))
  rx <- resample(rx, riso, filename = paste0(ofile, ".tif"), overwrite = TRUE)
  
  # Is this memory safe? get cell values
  dd <- as.data.frame(rx, xy = TRUE)
  names(dd) <- c("x", "y", stm)
  
  # convert from wide to long with dataframe, safer way?
  ddl <- melt(setDT(dd), id.vars = c("x","y"), value.name = var, variable = "Date")
  
  # add cellnumbers for using with join later
  xy <- as.matrix(ddl[,c("x","y")])
  ddl <- data.frame(id = cellFromXY(rx, xy), ddl, stringsAsFactors = FALSE)
  write_fst(ddl, path = paste0(ofile, "_interim.fst"))
  return(NULL)
}

# merge output tables 
mergeGCMdailyTable <- function(iso, model, experiment, gcmdir){
  cat("Processing", iso, model, experiment, "\n" )
  # search files
  dd <- list.files(file.path(gcmdir, "gcm_0_05deg_lat", iso), pattern = "_interim.fst", recursive = TRUE, full.names = TRUE)
  d <- grep(paste(model,experiment,sep = ".*"), dd, value = TRUE)
  df <- lapply(dd, function(x) data.table(read_fst(x)))
  # merge list of dataframes
  df <- Reduce(function(...) merge(..., by = c("id","x","y","Date"), all=TRUE), df)
  # convert to epoch time
  df$Date <- as.POSIXct(as.numeric(as.character(df$Date)), origin="1970-01-01")
  # save output
  outdir <- file.path(gcmdir, "gcm_0_05deg_lat", iso)
  ofile <- file.path(outdir, paste(iso,model,experiment, sep = "_"))
  write_fst(df, paste0(ofile, "_all_vars_table.fst"))
}

#############################################################################################
# Input parameters
gcmdir <- "~/data/CMIP6"
ff <- list.files(gcmdir, pattern = ".nc$", recursive = TRUE, full.names = TRUE)

# process each combination for specific country
experiments <- c("historical", "ssp585")
vars <- c("pr","tas","tasmax","tasmin")
models <- c("BCC-CSM2-MR","GFDL-ESM4","INM-CM5-0","MPI-ESM1-2-HR","MRI-ESM2-0")
sdate <- "1981-01-01"
edate <- "2010-12-31"
# reference raster with same geometry as CHIRPS 
ref <- rast(crs = "epsg:4326", extent = ext(c(-180, 180, -50, 50)), resolution = 0.05, vals = 1)

# TODO: add the country lists or WFP boundaries
isol <- c("HTI", "BDI")

# run all processes; can be run parallel for iso

for (iso in isol){
  for (var in vars){
    for (model in models){
      for (experiment in experiments){
        t1 <- Sys.time()
        getGCMdailyTable(iso, var, model, experiment, gcmdir, ff, sdate, edate, ref)
        cat("time to create table for", iso, var, model, experiment, Sys.time() - t1, "\n")
      }
    }
  }
}



for (iso in isol){
  for (model in models){
    for (experiment in experiments){
      t2 <- Sys.time()
      mergeGCMdailyTable(iso, model, experiment, gcmdir)
      cat("time to merge table for", iso, model, experiment, Sys.time() - t2, "\n")
    }
  }
}


# unit conversion needed