# test code in GCS
ifile <- "data/cmip6_index.csv"
idx <- read.csv(ifile, stringsAsFactors = FALSE)

# test one historical and one future
sidx <- idx[idx$source_id == "INM-CM5-0" & idx$variable_id %in% c("tasmax", "tasmin"), ]

downdir <- "~/data/"

# to avoid download time out?
options(timeout=180)

dwl <- lapply(1:nrow(sidx), getDataCMIP6, sidx, downdir, silent=FALSE, overwrite=FALSE)
dd <- do.call(rbind, dwl)

# files that failed download
sidx <- dd[dd$download_status == "FAIL" | dd$localfilechek == "FAIL",]

dwl <- lapply(1:nrow(sidx), getDataCMIP6, sidx, downdir, silent=FALSE, overwrite=TRUE)

dd <- do.call(rbind, dwl)
sidx <- dd[dd$download_status == "FAIL" | dd$localfilechek == "FAIL",]
