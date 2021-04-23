# Author: Ani Ghosh, Robert J. Hijmans 
# Date :  May 2020
# Version 0.1
# Licence GPL v3

# based on dismo::ecocrop with modifications suggested by Julian Ramirez
library(dismo)

getParsDismo <- function(v){
  crop <- new('ECOCROPcrop')
  crop@name  <- v$crop
  crop@famname <- "Unknown"
  crop@scientname <- "Unknown"
  crop@code <-  as.integer(0)
  crop@GMIN  <- as.numeric(v$Gmin)
  crop@GMAX   <- as.numeric(v$Gmax)
  crop@KTMP   <- as.numeric(v$Tkill)
  crop@TMIN   <- as.numeric(v$Tmin)
  crop@TOPMN  <- as.numeric(v$Topmin)
  crop@TOPMX  <- as.numeric(v$Topmax)
  crop@TMAX   <- as.numeric(v$Tmax)
  crop@RMIN   <- as.numeric(v$Rmin)
  crop@ROPMN  <- as.numeric(v$Ropmin)
  crop@ROPMX  <- as.numeric(v$Ropmax)
  crop@RMAX   <- as.numeric(v$Rmax)
  return(crop)
}


.getY <- function(a, x) {
  inter <- function(x1, y1, x2, y2, x) {
    y1 + (y2-y1) * (x-x1) / (x2-x1) 
  }
  y <- x
  if (is.null(a)) {
    y[] <- 1
  } else {
    y[] <- NA
    y[ x <= a[1] ] <- 0
    y[ x <= a[2] & x > a[1] ] <- inter(a[1], 0, a[2], 1, x[ x <= a[2] & x > a[1] ] )
    y[ x <= a[3] & x > a[2] ] <- 1
    y[ x <= a[4] & x > a[3] ] <- inter(a[3], 1, a[4], 0, x[ x <= a[4] & x > a[3] ] )
    y[ x >= a[4] ] <- 0
  }
  return(y)
}



.ecocropsingle <- function(crop, tmin, tavg, prec, rainfed, scale = 10) {
  
  if (rainfed) {
    # out <- c(NA,NA,NA,NA,NA,NA)
    nasum <- sum(is.na(c(tmin, tavg, prec)))
  } else { 
    # out <- c(NA,NA)
    nasum <- sum(is.na(c(tmin, tavg))) 
  }
  
  if (nasum > 0) {return(NA)}
  
  
  duration <- round((crop@GMIN + crop@GMAX) / 60) 
  tmp <- c(crop@TMIN, crop@TOPMN, crop@TOPMX, crop@TMAX)*scale
  tavgSuit <- .getY(tmp, tavg)
  ktmp <- c(crop@KTMP + 4*scale, crop@KTMP + 4*scale, Inf, Inf)
  tminSuit <- .getY(ktmp, tmin)
  
  # combine both temperature suitability
  tSuit <- cbind(tavgSuit, tminSuit)
  minv <-  apply(tSuit, 1, min)
  
  # temperature suitability starting each month across the growing season
  ecotf <- movingFun(minv, n=duration, fun=min, type='from', circular=TRUE) 
  
  # final temperature suitability
  tempFinSuit <- round(max(ecotf * 100))
  if (tempFinSuit == 0) {tempFinGS <- 0} else {tempFinGS <- max(which(ecotf == max(ecotf)))}
  
  if (rainfed) {
    pre <- c(crop@RMIN, crop@ROPMN, crop@ROPMX, crop@RMAX)
    # shftprec <- c(prec[12], prec[-12])
    cumprec <- movingFun(prec, n=duration, fun=sum, type='from', circular=TRUE) # + shftprec
    pSuit <- .getY(pre, cumprec)
    ecopf <- pSuit
    # final suitability for rainfed
    finSuit <- ecotf*ecopf*100
    finFinSuit <- round(max(finSuit))
    
    # suitability estimates 
    precFinSuit <- round(max(ecopf * 100))
    if (precFinSuit == 0) {precFinGS <- 0} else {precFinGS <- max(which(ecopf == max(ecopf)))}
    # combined suitability
    if (finFinSuit == 0) {finFinSuitGS <- 0} else {finFinSuitGS <- max(which(finSuit == max(finSuit)))}
    # now only returning the final suitability
    # return(c(finFinSuit, tempFinSuit, precFinSuit, finFinSuitGS, tempFinGS, precFinGS))
    return(finFinSuit)
  } else {
    # return(c(tempFinSuit, tempFinGS))
    return(tempFinSuit)
  }
}


.ecoSpatrun <- function(crop, tmin, tavg, prec, rainfed, filename='',...) { 
  
  out <- raster(tmin)
  filename <- trim(filename)
  
  big <- !canProcessInMemory(out, 3)
  
  if (big & filename == '') {
    filename <- rasterTmpFile()
  }
  
  if (filename != '') {
    out <- writeStart(out, filename, ...)
    todisk <- TRUE
  } else {
    vv <- matrix(ncol=nrow(out), nrow=ncol(out))
    todisk <- FALSE
  }
  
  bs <- blockSize(tmin)
  pb <- pbCreate(bs$n)
  
  if (todisk) {
    for (i in 1:bs$n) {
      
      tmp <- getValues(tavg, row=bs$row[i], nrows=bs$nrows[i])
      tmn <- getValues(tmin, row=bs$row[i], nrows=bs$nrows[i])
      if (rainfed) { 
        pre <- getValues(prec, row=bs$row[i], nrows=bs$nrows[i])
      }
      
      v <- vector(length=nrow(tmp))
      v[] <- NA
      
      nac <- which(!is.na(tmn[,1]))
      
      for (j in nac) {
        if(sum(is.na(tmp[j,])) == 0) {
          e <- .ecocropsingle(crop, tmn[j,], tmp[j,], pre[j,], rainfed=rainfed)
          v[j] <- e 
        }
      }
      
      out <- writeValues(out, v, bs$row[i])
      pbStep(pb, i)
    }
    
    out <- writeStop(out)
    
  } else {
    
    for (i in 1:bs$n) {
      tmp <- getValues(tavg, row=bs$row[i], nrows=bs$nrows[i])
      tmn <- getValues(tmin, row=bs$row[i], nrows=bs$nrows[i])
      if (rainfed) { 
        pre <- getValues(prec, row=bs$row[i], nrows=bs$nrows[i])
      }
      
      v <- vector(length=nrow(tmp))
      v[] <- NA
      
      nac <- which(!is.na(tmn[,1]))
      
      for (j in nac) {
        if(sum(is.na(tmp[j,])) == 0) {
          e <- .ecocropsingle(crop, tmn[j,], tmp[j,], pre[j,], rainfed=rainfed)
          v[j] <- e 
        }
      }
      
      cols <- bs$row[i]:(bs$row[i]+bs$nrows[i]-1)
      vv[,cols] <- matrix(v, nrow=out@ncols)
      pbStep(pb, i)
    }
    out <- setValues(out, as.vector(vv))
  }
  pbClose(pb)
  return(out)
}


ecocropMod <- function(crop, tmin, tavg, prec, rainfed,...) {
  if (missing(prec) & rainfed) {
    stop('prec missing while rainfed=TRUE' )
  }
  
  if (inherits(tmin, 'Raster')) {
    if (nlayers(tmin) != 12) {
      stop()
    }
    .ecoSpatrun(crop, tmin, tavg, prec, rainfed,...)
  } else {
    .ecocropsingle(crop=crop, tmin=tmin, tavg=tavg, prec=prec, rainfed=rainfed,...)
  }
}