# BDI plots

# prepare the zones first

library(raster)
vdir <- "G:\\.shortcut-targets-by-id\\1zTxTZGK0LOQdQnN8N2TGWOP0PBUY-aMy\\shps"
v <- list.files(vdir, pattern = glob2rx("*livelihoodzones_geonode*.shp$"), recursive = TRUE, full.names = TRUE)
iso3 <- "BDI"
v1 <- grep(tolower(iso3), v, value = TRUE)
v1 <- shapefile(v1)

# we will work at the adm 1 level
z1 <- v1[v1$LZ_Name %in% "Plaine Imbo", ]
z1$LZNAMEF <- "Plaine Imbo"

z2 <- v1[v1$LZ_Name %in% "Depression de l'Est", ]
z2$LZNAMEF <- "Depression de l'Est"

z3 <- v1[v1$LZ_Name %in% "Depression du Nord", ]
z3$LZNAMEF <- "Depression du Nord"

z4 <- v1[v1$LZ_Name %in% "Plateaux Secs de l'Est", ]
z4$LZNAMEF <- "Plateaux Secs de l'Est"

# combine all
bdi <- bind(z1, z2, z3, z4)
vs <- bdi

#####################################################################################
dir <- "G:\\My Drive\\work\\ciat\\ecocrop"
eco <- readxl::read_excel("suitability/ecocrop_runs.xlsx", sheet = 3)
eco <- eco[eco$ISO == "BDI",]


outdir <- "G:\\My Drive\\work\\ciat\\wfp"

lapply(1:nrow(eco), makePlots, eco, vs, dir, outdir)
