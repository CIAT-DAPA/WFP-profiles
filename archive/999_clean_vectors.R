# combine WFP boundaries
library(sf)
library(raster)

vdir <- "G:\\.shortcut-targets-by-id\\1zTxTZGK0LOQdQnN8N2TGWOP0PBUY-aMy\\shps"
vv <- list.files(vdir, pattern = glob2rx("*_regions*.shp$"), recursive = TRUE, full.names = TRUE)
# only keep the tza new
vv <- grep("old", vv, invert = TRUE, value = TRUE)

x <- list()
for (i in 1:length(vv)){
  v <- st_read(vv[i])
  x[[i]] <- names(v)
}
data.table::rbindlist(x)

# standardize by each country
v1 <- shapefile(vv[1])
v1 <- v1[, c("LZ_Code", "LZ_Name", "region", "zone")]
v1$iso3 <- "BDI"

v2 <- shapefile(vv[2])
v2 <- v2[, c("GID_0", "region", "zone")]
names(v2) <- c("iso3", "region", "zone")

v3 <- shapefile(vv[3])
v3 <- v3[, c("LZCODE", "LZNAMEEN" ,"region", "zone")]
names(v3) <- c("LZ_Code", "LZ_Name" ,"region", "zone")
v3$iso3 <- "GIN"

v4 <- shapefile(vv[4])
v4 <- v4[, c("LZCODE", "LZNAMEE" ,"region", "zone")]
names(v4) <- c("LZ_Code", "LZ_Name" ,"region", "zone")
v4$iso3 <- "HTI"

v5 <- shapefile(vv[5])
v5 <- v5[, c("iso3","region", "zone")]

v6 <- shapefile(vv[6])
v6 <- v6[, c("PredLHZ_EN","region", "zone")]
names(v6) <- c("LZ_Name" ,"region", "zone")
v6$iso3 <- "NER"

v7 <- shapefile(vv[7])
v7 <- v7[, c("iso3","region", "zone")]


v8 <- shapefile(vv[8])
v8 <- v8[, c("LZNAMEEN","region", "zone")]
names(v8) <- c("LZ_Name" ,"region", "zone")
v8$iso3 <- "SOM"


v9 <- shapefile(vv[9])
v9 <- v9[, c("GID_0","region", "zone")]
names(v9) <- c("iso3" ,"region", "zone")

va <- bind(v1, v2, v3, v4, v5, v6, v7, v8, v9)
shapefile(va, "data/all_zones_countries.shp")
