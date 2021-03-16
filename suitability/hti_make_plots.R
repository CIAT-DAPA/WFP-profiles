# HTI plots

# prepare the zones first

library(raster)
vdir <- "G:\\.shortcut-targets-by-id\\1zTxTZGK0LOQdQnN8N2TGWOP0PBUY-aMy\\shps"
v <- list.files(vdir, pattern = glob2rx("*livelihoodzones*geonode*.shp$"), recursive = TRUE, full.names = TRUE)
iso3 <- "HTI"
v1 <- grep(tolower(iso3), v, value = TRUE)
v1 <- shapefile(v1)

# we will work at the adm 1 level
zone1 <- c("Dodoma", "Singida", "Tabora")
zone2 <- c( "Geita", "Kagera", "Mara", "Mwanza", "Shinyanga", "Simiyu")
zone3 <- c("Arusha", "Kilimanjaro", "Manyara", "Tanga")

z1 <- v1[v1$adm1_name %in% zone1, ]
z1 <- aggregate(z1, by = "iso3")
z1$LZNAMEF <- "Zone 1"


z2 <- v1[v1$adm1_name %in% zone2, ]
z2 <- aggregate(z2, by = "iso3")
z2$LZNAMEF <- "Zone 2"


z3 <- v1[v1$adm1_name %in% zone3, ]
z3 <- aggregate(z3, by = "iso3")
z3$LZNAMEF <- "Zone 3"

# combine all
tza <- bind(z1, z2, z3)
vs <- tza

#####################################################################################
dir <- "G:\\My Drive\\work\\ciat\\ecocrop"
eco <- readxl::read_excel("suitability/ecocrop_runs.xlsx", sheet = 3)
eco <- eco[eco$ISO == "TZA",]

outdir <- "G:\\My Drive\\work\\ciat\\wfp"

lapply(1:nrow(eco), makePlots, eco, vs, dir, outdir)
