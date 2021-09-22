# -------------------------------------------------- #
# Climate Risk Profiles -- Get soil capacity data
# H. Achicanoy
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

# Input parameters:
#   crd: data frame with id, x, and y columns
#   root_depth: root depth in cm (it's assumed to be constant over all coordinates)
#   outfile: output file path
# Output:
#   Data frame with soil capacity and soil saturation values per pixel
get_soil <- function(crd = crd, root_depth = 60, outfile = './soilcp_data.fst'){
  
  if(!file.exists(outfile)){
    # Load packages
    if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
    suppressMessages(pacman::p_load(raster, tidyverse, fst, GSIF, vroom))
    
    # Load CHIRPS template
                           #//catalogue/BaseLineDataCluster01/observed/gridded_products/chirps/daily/chirps-v2.0.2020.01.01.tif
    tmp <- raster::raster("//CATALOGUE/Workspace14/WFP_ClimateRiskPr/1.Data/chirps-v2.0.2020.01.01.tif")
    
    # Transform crd to raster study area
    r <- raster::rasterFromXYZ(xyz = crd[,c('x','y')] %>% unique %>% dplyr::mutate(vals = 1),
                               res = raster::res(tmp),
                               crs = raster::crs(tmp))
    
    # Soil data repository. ISRIC soil data 250 m
    soils_root <- '//catalogue/BaseLineData_cluster04/GLOBAL/Biofisico/SoilGrids250m'
    # Soil organic carbon content
    orc <- raster::stack(list.files(paste0(soils_root,'/Chemical soil properties/Soil organic carbon content'), pattern = '.tif$', full.names = T) %>% sort())
    # Cation exchange capacity
    cec <- raster::stack(list.files(paste0(soils_root,'/Chemical soil properties/Cation exchange capacity (CEC)'), pattern = '.tif$', full.names = T) %>% sort())
    # Soil ph in H2O
    phx <- raster::stack(list.files(paste0(soils_root,'/Chemical soil properties/Soil ph in H2O'), pattern = '.tif$', full.names = T) %>% sort())
    # Sand content
    snd <- raster::stack(list.files(paste0(soils_root,'/Physical soil properties/Sand content'), pattern = '.tif$', full.names = T) %>% sort())
    # Silt content
    slt <- raster::stack(list.files(paste0(soils_root,'/Physical soil properties/Silt content'), pattern = '.tif$', full.names = T) %>% sort())
    # Clay content
    cly <- raster::stack(list.files(paste0(soils_root,'/Physical soil properties/Clay content (0-2 micro meter) mass fraction'), pattern = '.tif$', full.names = T) %>% sort())
    # Bulk density
    bld <- raster::stack(list.files(paste0(soils_root,'/Physical soil properties/Bulk density (fine earth)'), pattern = '.tif$', full.names = T) %>% sort())
    
    # Put all layers together and resampling them to the proper resolution 5 km
    soil <- raster::stack(orc,cec,phx,snd,slt,cly,bld)
    soil <- soil %>%
      raster::crop(., raster::extent(r)) %>%
      raster::resample(., r) %>%
      raster::mask(., mask = r)
    
    # Obtain soil data for the corresponding coordinates
    soil_data <- cbind(crd, raster::extract(soil, crd[,c('x','y')]))
    
    # Arrange the soil data at different depth levels
    soil_data2 <- soil_data %>%
      tidyr::gather(key = 'var', value = 'val', -(1:3)) %>%
      tidyr::separate(col = 'var', sep = '_M_', into = c('var','depth')) %>%
      tidyr::spread(key = 'var', value = 'val') %>%
      dplyr::arrange(id)
    soil_data2$depth <- gsub('_250m_ll','',soil_data2$depth)
    
    # Save this table FIX THIS
    # fst::write_fst(soil_data2, '//dapadfs.cgiarad.org/workspace_cluster_8/climateriskprofiles/data/soil_data.fst')
    
    # Get Available soil water capacity per depth level
    soil_data2 <- cbind(soil_data2,GSIF::AWCPTF(SNDPPT = soil_data2$SNDPPT,
                                                SLTPPT = soil_data2$SLTPPT,
                                                CLYPPT = soil_data2$CLYPPT,
                                                ORCDRC = soil_data2$ORCDRC,
                                                BLD = soil_data2$BLDFIE,
                                                CEC = soil_data2$CECSOL,
                                                PHIHOX = soil_data2$PHIHOX/10,
                                                h1=-10, h2=-20, h3=-33))
    
    #now calculate the ASW in mm for each soil horizon
    soil_data2$tetaFC <- soil_data2$WWP + soil_data2$AWCh3 #volumetric water content at field capacity (fraction)
    soil_data2$AWSat <- soil_data2$tetaS - soil_data2$tetaFC
    
    soil_data2$depth[soil_data2$depth == "sl1"] <- 0
    soil_data2$depth[soil_data2$depth == "sl2"] <- 5
    soil_data2$depth[soil_data2$depth == "sl3"] <- 15
    soil_data2$depth[soil_data2$depth == "sl4"] <- 30
    soil_data2$depth[soil_data2$depth == "sl5"] <- 60
    soil_data2$depth[soil_data2$depth == "sl6"] <- 100
    soil_data2$depth[soil_data2$depth == "sl7"] <- 200
    soil_data2$depth <- as.numeric(soil_data2$depth)
    
    soilcap_calc <- function(x, y, rdepth=60, minval, maxval) {
      if (length(x) != length(y)) {stop("length of x and y must be the same")}
      rdepth <- max(c(rdepth,minval)) #cross check
      rdepth <- min(c(rdepth,maxval)) #cross-check
      wc_df <- data.frame(depth=y,wc=x)
      if (!rdepth %in% wc_df$depth) {
        wc_df1 <- wc_df[which(wc_df$depth < rdepth),]
        wc_df2 <- wc_df[which(wc_df$depth > rdepth),]
        y1 <- wc_df1$wc[nrow(wc_df1)]; y2 <- wc_df2$wc[1]
        x1 <- wc_df1$depth[nrow(wc_df1)]; x2 <- wc_df2$depth[1]
        ya <- (rdepth-x1) / (x2-x1) * (y2-y1) + y1
        wc_df <- rbind(wc_df1,data.frame(depth=rdepth,wc=ya),wc_df2)
      }
      wc_df <- wc_df[which(wc_df$depth <= rdepth),]
      wc_df$soilthick <- wc_df$depth - c(0,wc_df$depth[1:(nrow(wc_df)-1)])
      wc_df$soilcap <- wc_df$soilthick * wc_df$wc
      soilcp <- sum(wc_df$soilcap) * 10 #in mm
      return(soilcp)
    }
    
    soil_data4 <- soil_data2 %>%
      dplyr::group_by(id) %>%
      dplyr::group_split(id) %>%
      purrr::map(.f = function(px){
        scp  <- soilcap_calc(x=px$AWCh3, y=px$depth, rdepth = root_depth, minval=45, maxval=100)
        ssat <- soilcap_calc(x=px$AWSat, y=px$depth, rdepth = root_depth, minval=45, maxval=100)
        df <- data.frame(id = unique(px$id),
                         x  = unique(px$x),
                         y  = unique(px$y),
                         scp  = scp,
                         ssat = ssat)
        return(df)
      }) %>%
      dplyr::bind_rows()
    
    dir.create(path = dirname(outfile), FALSE, TRUE)
    fst::write_fst(x = soil_data4, path = outfile)
  } else {
    cat('Soil capacity already calculated.\n')
  }
  return(cat('Get soil data: finished successfully!\n'))
}
