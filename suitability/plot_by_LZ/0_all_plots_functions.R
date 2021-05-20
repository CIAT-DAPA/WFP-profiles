# scripts used to make suitability plots shared in August, 2020
# make plots
library(raster)
library(sf)
library(tmap)
library(tmaptools)
library(grid)
library(RColorBrewer)
library(cowplot)
library(tidyverse)
library(officer)

# tmap_mode("plot")

# Suitability change plot
#######################################################################################
# Suitability change plot
changeFunction <- function(a,b){
  # becomes suitable
  c1 <- ifelse(a<40 & b>=40, 1, 0)
  # stay suitable but above current
  c2 <- ifelse(a<b & a>=40 & b>=40, 2, 0)
  #  stay suitable equal to current 
  c3 <- ifelse(a==b & a>40 & b>40, 3, 0)
  # stay suitable but below current
  c4 <- ifelse(a>b & a>=40 & b>=40, 4, 0)
  # becomes unsuitable
  c5 <- ifelse(a>=40 & b<40, 5, 0)
  # both unsuitable
  c6 <- ifelse(a>=0 & a<40 & b>=0 & b<40, 6, 0)
  # combine conditions
  cnd <- apply(cbind(c1,c2,c3,c4,c5,c6), 1, FUN = function(x){sum(x, na.rm = TRUE)})
  return(cnd)
}

####################################################################################
createSuitClass <- function(r){
  m <- c(-Inf, 0, 1,  0, 40, 2, 40, 80, 3, 80, Inf, 4)
  rclmat <- matrix(m, ncol=3, byrow=TRUE)
  rc <- reclassify(r, rclmat)
  return(rc)
}

######################################################################################
makeSuitGroupMap <- function(r, lbls, pcols, lshow, tlab){
  
  cc <- sampleRandom(r, length(lbls), cells=TRUE)
  r[cc[,1]] <- 1:length(lbls)
  
  suitmap <- tm_shape(r) +
    tm_raster(style = "cat", labels = lbls,
              palette = pcols, title = "suitability(%)",
              legend.show = lshow,
              legend.is.portrait = FALSE) + 
    tm_layout(main.title = tlab,
              main.title.size = 1.5,  
              frame = FALSE,
              main.title.position = "center",
              main.title.fontface = 1,
              inner.margins=c(0.03,0.03,0.03,0.03))
  # tm_compass(type = "arrow", position = c("right", "top"), size=1, text.size=0.8) +
  # tm_scale_bar(position=c("left","bottom"), width = 0.2, lwd = 0.3, size = 0.5)
  suitmap
}

makeSuitGroupLegend <- function(r, lbls, pcols, portrait){
  
  cc <- sampleRandom(r, length(lbls), na.rm=TRUE, cells=TRUE)
  r[cc[,1]] <- 1:length(lbls)
  
  suitmaplegend <- tm_shape(r) +
    tm_raster(style = "cat", labels = lbls,
              palette = pcols, title = "Suitability groups",
              legend.is.portrait = portrait) + 
    tm_layout(legend.only = TRUE,
              legend.title.size = 1.5,
              legend.text.size = 1,
              legend.width = 1,
              legend.height = 0.5,
              # legend.outside = TRUE,
              # legend.outside.position = "top",
              # legend.outside.size = 1,
              legend.bg.color = "transparent",
              legend.bg.alpha = 1)
  suitmaplegend
}

###################################################################################################
makeChangeMap <- function(chg, lbls, pcols, lshow, tlab){
  # ensure all classes are present
  chg[chg==0]<- NA
  cc <- sampleRandom(chg, 6, cells=TRUE)
  chg[cc[,1]] <- 1:6
  
  chgmap <- tm_shape(chg) +
    tm_raster(style="cat", labels = lbls,
              palette = pcols, legend.show = lshow,
              title = "Suitability change") + 
    tm_layout(main.title = tlab,
              main.title.size = 1.5,  
              frame = FALSE,
              main.title.position = "center",
              main.title.fontface = 1,
              inner.margins=c(0.03,0.03,0.03,0.03))
  chgmap
}


makeChangeMapLegend <- function(chg, lbls, pcols, portrait){
  # ensure all classes are present
  chg[chg==0]<- NA
  cc <- sampleRandom(chg, 6, na.rm=TRUE, cells=TRUE)
  chg[cc[,1]] <- 1:6
  
  chgmaplegend <- tm_shape(chg) +
    tm_raster(style="cat", labels = lbls,
              palette = pcols,
              title = "Suitability change",
              legend.is.portrait = portrait) + 
    tm_layout(legend.only = TRUE,
              legend.title.size = 1.5,
              legend.text.size = 1,
              legend.width = 1,
              legend.height = 0.5,
              # legend.outside = TRUE,
              # legend.outside.position = "top",
              # legend.outside.size = 1,
              legend.bg.color = "transparent",
              legend.bg.alpha = 1)
  
  chgmaplegend
}

#############################################################################################
# barplot
makeChartByProvince <- function(s, vr, brs, cat, fill, xlabs){
  # compute area in each category for each province
  vs <- vr[s,]
  abr <- lapply(brs, FUN = function(r,vs){r <- mask(r,vs); data.frame(t(tapply(area(r), r[], sum)))}, vs)
  # create a data.frame
  abrs <- data.frame(data.table::rbindlist(abr, fill = TRUE))
  # compute % stat
  abrs <- t(abrs)
  abrs <- abrs[!is.na(row.names(abrs)), ,drop = FALSE]
  abrs <- abrs*100/colSums(abrs, na.rm = TRUE)
  abrs <- round(abrs, 2)
  abrs[is.na(abrs)] <- 0
  
  colnames(abrs) <- xlabs
  abrs <- data.frame(cat = row.names(abrs), 
                     abrs, stringsAsFactors = FALSE, row.names = NULL)
  
  # color category for all
  nms <- data.frame(xx = paste0("X",1:length(cat)), lbl = cat, fill = fill, stringsAsFactors = FALSE)
  
  # merge colors and tables
  abrsn <- merge(abrs, nms, by.x = 'cat', by.y = "xx", all = TRUE)
  
  # ensure legend order
  abrsn$cat <- factor(abrsn$cat, levels = abrsn$cat)
  
  # wide to long
  vls85 <- abrsn %>% gather(clim, area, -c(cat, lbl, fill))
  vls85$area[is.na(vls85$area)] <- 0
  
  # rcp 85
  # vls85 <- vls[vls$clim %in% c("hist", "rcp_8_5_2030", "rcp_8_5_2050"),]
  # name of the province
  uname <- vs$pnames
  
  plot85 <- ggplot() + geom_bar(aes(y = area, x = clim, fill = cat), data = vls85,
                                stat="identity", width = 0.5) +
    scale_fill_manual(values=vls85$fill) +
    labs(title = uname, x=NULL, y=NULL) +
    scale_y_continuous(name="Cumulative percentage of area (%)", limits=c(-5, 105)) +
    scale_x_discrete(labels=xlabs) +
    theme_bw() +
    theme(legend.position = "none", axis.title.y = element_text(size=10),
          axis.text.x = element_text(size=12), plot.title = element_text(size=14))
  plot85
}

#############################################################################################

makePlotSingle <- function(admin,level,vs,f,pdir){
  
  # cat("processing ", unique(v$iso3), "\n")
  
  # # in multiple admin units are present
  # if(grepl(",",admin)){
  #   admin <- trimws(unlist(strsplit(admin, ",")))
  # }
  # adnm <- unique(switch(level+1, v$NAME_0, v$NAME_1, v$NAME_2, v$NAME_c)) # v$NAME_c is combined
  # vr <- v[adnm %in% admin,]
  vr <- vs[vs$LZNAMEF == admin,]
  # 
  # get corresponding layer names
  nm <- gsub(".tif","_stackname.rds" ,f)
  
  rs <- stack(f)
  nm <- readRDS(nm)
  names(rs) <- nm
  
  # crop by roi but expand the extent to include more context
  rr <- crop(rs, extent(vr)*1.25)
  # change resolution to make the plot look better
  rr <- disaggregate(rr, fact = 5)
  rr <- mask(rr, vr)
  
  # border of the focus province
  # pnames <- get('vr')[[paste0('NAME_',level)]]
  vr$pnames <- substr(vr$LZNAMEF, 1, 15)
  border <- tm_shape(vr) + tm_borders(lwd=2, col=gray(0.1)) + tm_text("pnames", size = 1)
  # all border
  # allborder <- tm_shape(v) + tm_borders(lwd=1.25, col=gray(0.3), lty = "dashed")
  
  hist <- subset(rr, grep("current",names(rr)))
  # r452030 <- subset(rr, grep("4_5_2030",names(rr)))
  r852030 <- subset(rr, grep("8_5_2030",names(rr)))
  # r452050 <- subset(rr, grep("4_5_2050",names(rr)))
  r852050 <- subset(rr, grep("8_5_2050",names(rr)))
  
  
  ################################################################################################
  # suitability map and chart
  # first create the broad category raster
  brs <- lapply(c(hist,r852030,r852050),createSuitClass)
  
  ######################################################################################
  # more data prep for plots
  # barplot: compute area in each category for each province
  onames <- paste(vr$pnames, collapse = "_")
  
  cname2 <- file.path(pdir, paste0(onames, "_suitability_group_maps_", gsub(".tif",".png",basename(f))))
  
  ############################# map #################################################
  suitcat <- c("unsuitable(0%)", "poor suit (0-40%)", 
               "mod suit (40-80%)", "high suit (80-100%)")
  fills <- rev(c("#1a9850", "#d9ef8b", "#f4a582", "#bababa"))
  
  p21 <- makeSuitGroupMap(brs[[1]],suitcat,fills,FALSE,"Historical \n(1960-90)") + border
  p22 <- makeSuitGroupMap(brs[[2]],suitcat,fills,FALSE,"RCP 8.5 \n(2030)") + border
  p23 <- makeSuitGroupMap(brs[[3]],suitcat,fills,FALSE,"RCP 8.5 \n(2050)") + border
  
  ############################# chart #################################################
  
  if (nrow(vr)>1){
    plotbarlist <- lapply(1:nrow(vr), makeChartByProvince, vr, brs, suitcat, fills,
                          xlabs = c("hist", "2030", "2050"))
    # remove y label from all plots, except the left most
    plotbarlist[-1] <- lapply(plotbarlist[-1], 
                              function(pp) return(pp+theme(axis.title.y = element_blank(),axis.text.y = element_blank())))
    
    barlist <- ggpubr::ggarrange(plotlist = plotbarlist, ncol=length(plotbarlist))
    
    # for multiple polygon, keep legend in landscape
    plg2 <- makeSuitGroupLegend(brs[[1]],suitcat,fills, FALSE)
    
    png(cname2, width = 8, height = 8, units = "in", res = 300)
    grid.newpage()
    page.layout <- grid.layout(nrow = 3, ncol = 3, widths=c(rep(1/3,3)), heights=c(0.6,0.25,0.15))
    pushViewport(viewport(layout = page.layout))
    print(p21, vp=viewport(layout.pos.row = 1, layout.pos.col = 1))
    print(p22, vp=viewport(layout.pos.row = 1, layout.pos.col = 2))
    print(p23, vp=viewport(layout.pos.row = 1, layout.pos.col = 3))
    print(barlist, vp=viewport(layout.pos.row = 2, layout.pos.col = 1:3))
    print(plg2, vp=viewport(layout.pos.row = 3, layout.pos.col = 1:3))
    dev.off()
    
  } else {
    plotbarlist <- makeChartByProvince(nrow(vr),vr,brs, suitcat, fills, xlabs = c("hist", "2030", "2050"))
    
    # for single polygon, keep legend in portrait
    plg2 <- makeSuitGroupLegend(brs[[1]],suitcat,fills, TRUE)
    
    png(cname2, width = 10, height = 8, units = "in", res = 300)
    grid.newpage()
    page.layout <- grid.layout(nrow = 2, ncol = 3, widths=c(rep(1/3,3)), heights=c(0.6,0.4))
    pushViewport(viewport(layout = page.layout))
    print(p21, vp=viewport(layout.pos.row = 1, layout.pos.col = 1))
    print(p22, vp=viewport(layout.pos.row = 1, layout.pos.col = 2))
    print(p23, vp=viewport(layout.pos.row = 1, layout.pos.col = 3))
    print(plotbarlist, vp=viewport(layout.pos.row = 2, layout.pos.col = 1))
    print(plg2, vp=viewport(layout.pos.row = 2, layout.pos.col = 2:3))
    dev.off()
  }
  
  
  
  ################################################################################################
  # change map and chart
  # compute change layers
  cgrs <- lapply(c(r852030,r852050), FUN = function(r,ref){overlay(ref, r, fun = changeFunction)}, hist)
  
  ############################# map #################################################
  # all possible col and cat
  chgcat <- c("new suit", "more suit", "same suit", "less suit", "new unsuit", "no suit")
  fillc <- c("#8073ac", "#d8daeb", "#ffffbf", "#fdb863", "#b35806","#bababa")
  
  p31 <- makeChangeMap(cgrs[[1]],chgcat,fillc,FALSE,"RCP 8.5 \n(2030)") +  border
  p32 <- makeChangeMap(cgrs[[2]],chgcat,fillc,FALSE,"RCP 8.5 \n(2050)") + border
  
  
  cname3 <- file.path(pdir, paste0(onames, "_suitability_change_analysis_", gsub(".tif",".png",basename(f))))
  
  if (nrow(vr)>1){
    chgbarlist <- lapply(1:nrow(vr), makeChartByProvince, vr, cgrs, chgcat, fillc,
                         xlabs = c("2030", "2050"))
    
    # remove y label from all plots, except the left most
    chgbarlist[-1] <- lapply(chgbarlist[-1], 
                             function(pp) return(pp+theme(axis.title.y = element_blank(),axis.text.y = element_blank())))
    
    chgbarlist <- ggpubr::ggarrange(plotlist = chgbarlist, ncol=length(chgbarlist))
    
    # for multiple polygon, keep legend in landscape
    plg3 <-  makeChangeMapLegend(cgrs[[1]],chgcat,fillc,FALSE)
    
    png(cname3, width = 8, height = 8, units = "in", res = 300)
    grid.newpage()
    page.layout <- grid.layout(nrow = 3, ncol = 2, widths=c(0.5,0.5), heights=c(0.6,0.25,0.15))
    pushViewport(viewport(layout = page.layout))
    print(p31, vp=viewport(layout.pos.row = 1, layout.pos.col = 1))
    print(p32, vp=viewport(layout.pos.row = 1, layout.pos.col = 2))
    print(chgbarlist, vp=viewport(layout.pos.row = 2, layout.pos.col = 1:2))
    print(plg3, vp=viewport(layout.pos.row = 3, layout.pos.col = 1:2))
    dev.off()
    
  } else {
    chgbarlist <- makeChartByProvince(nrow(vr),vr,cgrs, chgcat, fillc, 
                                      xlabs = c("2030", "2050"))
    
    # for single polygon, keep legend in portrait
    plg3 <-  makeChangeMapLegend(cgrs[[1]],chgcat,fillc,TRUE)
    
    png(cname3, width = 8, height = 8, units = "in", res = 300)
    grid.newpage()
    page.layout <- grid.layout(nrow = 2, ncol = 2, widths=c(0.5,0.5), heights=c(0.6,0.4))
    pushViewport(viewport(layout = page.layout))
    print(p31, vp=viewport(layout.pos.row = 1, layout.pos.col = 1))
    print(p32, vp=viewport(layout.pos.row = 1, layout.pos.col = 2))
    print(chgbarlist, vp=viewport(layout.pos.row = 2, layout.pos.col = 1))
    print(plg3, vp=viewport(layout.pos.row = 2, layout.pos.col = 2))
    dev.off()
  }
}


makePlots <- function(i, eco, vs, dir, outdir){
  ecos <- eco[i,]
  
  # dissolve situations are tricky
  # if(ecos$dissolve){next}
  
  # country name
  iso <- ecos$ISO
  
  cat("processing ", iso, "\n")
  
  # sub-national unit name
  roi <- ecos$region_name
  
  # data/outputfolder
  fdir <- file.path(outdir, iso)
  # create a png folder
  pdir <- file.path(fdir, "png")
  dir.create(pdir, showWarnings = FALSE, recursive = TRUE) 
  
  # LZ boundary
  roi <- vs$LZNAMEF
  
  # list of crops
  crp <- ecos$value_chain
  
  datadir <- file.path(dir, "result/2_5min")
  # list all crop tif files
  ff <- list.files(datadir, pattern = glob2rx(paste0(iso, "*.tif$")), full.names = TRUE, recursive = TRUE)
  
  # will go to another function
  # read one crop
  for (f in ff){
    cat("processing ", f, "\n")
    lapply(roi,makePlotSingle,level,vs,f,pdir)
  }
}


##################################################################################################
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
