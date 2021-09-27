# WFP Climate Risk Project
# =----------------------
# interpolation  
# A. Esquivel - H. Achicanoy - C.Saavedra 
# =----------------------
#shp with new regions
regions_all = raster::shapefile("//CATALOGUE/Workspace14/WFP_ClimateRiskPr/1.Data/shps/niger/ner_regions_v2/ner_regions2.shp")
# =----------------------
# tabla de indices
tbl <- to_graph
# data chirps  
tmp <- raster::raster(paste0(root,'/1.Data/chirps-v2.0.2020.01.01.tif'))
tmp_c  <- raster::crop(tmp, raster::extent(regions_all))
# altitude for LVZ of Niger
alt <- raster::getData('alt', country = iso, path = paste0(root,'/1.Data/shps/',country))
alt <- raster::resample(x = alt, y = tmp_c)
alt <- raster::crop(alt, raster::extent(regions_all)) %>% raster::mask(., regions_all)
# list of periods 
list_time <- unique(tbl$time1)
# =----------------------
tbl_it <- list_time %>% 
  purrr::map(.f=function(i){
    # tabla filtrada por periodo de tiempo
    #i=1
    tbl_flt <- tbl %>% dplyr::filter(time1==list_time[i])
    # indices 
    indices <- c("ATR","AMT","SPI","TAI","NDD","NDWS","P5D","P95","NWLD",
                 "NT_X","SHI","THI_0","THI_1","THI_2","THI_3","THI_23",
                 "LGP","SLGP","SLGP_CV","NWLD_max","NWLD50_max","NWLD90_max") 
    # uso rasterize para crear el raster con la tabla indices.fst
    rd_data <- rasterize(x=tbl_flt[, c('x','y')], # lon-lat(x,y) de indices.fst
                         y=alt, # raster rd_obj
                         field= tbl_flt[,indices], # agregar variables al raster
                         fun=mean) # aggregate function
    # =----------------------   
    # función de interpolación
    fill.interpolate <- function(rd_data, alt){
      # coordenadas
      xy <- data.frame(xyFromCell(rd_data, 1:ncell(rd_data))) 
      # valores de Y                    
      vals <- raster::getValues(rd_data)
      # add elevation
      p <- raster::aggregate(alt, res(rd_data)/res(alt))
      # remove NAs
      set.seed(1)
      #i <- !is.na(vals)
      i <- complete.cases(vals)
      # filtro para coordenadas sin NA
      xy <- xy[i,]
      # filtra para la variable de respuesta (sin NA)
      vals <- vals[i,]
      # Muestreo del 20% de pixels
      k <- sample(1:nrow(xy), nrow(xy)*0.2)
      # filtr pixels de la muestra
      xy <- xy[k,]
      vals <- (vals[k,])
      vals <- round(vals)
      r1 <- alt
      r1[] <- NA
      #### Thin plate spline model
      interpolations <- lapply(X = 1:ncol(vals), FUN = function(j){
        tps <- fields::Tps(xy, vals[,j])
        # use model to predict values at all locations
        p <- raster::interpolate(r1, tps, ext = extent(regions_all))
        p <- raster::mask(p, alt)
        return(p)
      })
    }
    interpolations <- fill.interpolate(rd_data,alt)
    # transform in raster stack
    interpolations <- raster::stack(interpolations)
    #add indices names
    names(interpolations) <- indices
    tbl_it <- base::data.frame(raster::rasterToPoints(interpolations))
    tbl_it$id <- as.numeric(raster::cellFromXY(object = tmp, xy = tbl_it[,c('x','y')]))
    tbl_it$time1 <- i
    return(tbl_it)
  })
# =----------------------
tbl_it
# extract indices for periods of time 
fut1_intp <- as.data.frame(tbl_it[[1]])
fut2_intp <- as.data.frame(tbl_it[[2]])
his_intp <- as.data.frame(tbl_it[[3]])

tbl_intp <- rbind(his_intp,fut1_intp,fut2_intp) 
tbl_intp$time1 <- as.character(tbl_intp$time1)
tbl_intp <- tbl_intp %>% dplyr::mutate(time = dplyr::case_when(time1 %in%  "1" ~ 'Historic', 
                                                               time1 %in%  "2" ~ '2021-2040',
                                                               time1 %in%  "3" ~ '2041-2060',
                                                               TRUE ~ time1))
tbl_intp$time1 <- as.numeric(tbl_intp$time1)
tbl_intp <- tbl_intp %>% 
  dplyr::group_by(time,time1,id,x,y)
# filter new id 
px_n <- base::setdiff(x=tbl_intp$id, y=to_graph$id)
# table with only new id's
tbl_intp <- subset(tbl_intp, tbl_intp$id %in% px_n==T)
# =----------------------
# join table of real and interpolated indices
to_graph <- rbind(to_graph, tbl_intp) 
# =----------------------