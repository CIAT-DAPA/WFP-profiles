# -------------------------------------------------- #
# Climate Risk Profiles -- Hazard counts and bivariate choropleth map
# H. Achicanoy & A. Esquivel
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

options(warn = -1, scipen = 999)
if(!require(pacman)){install.packages('pacman'); library(pacman)} else {suppressMessages(library(pacman))}
suppressMessages(pacman::p_load(tidyverse,pals,fst,raster,terra,RColorBrewer))

root <- '//dapadfs.cgiarad.org/workspace_cluster_14/WFP_ClimateRiskPr'
iso     <- 'HTI'
country <- 'Haiti'

# Read hazard counts table
tbl <- fst::read_fst(paste0(root,'/7.Results/',country,'/results/maps/all_s1/HZ.fst'))

if('El cruce entre dos grupos existe'){
  # Cross table between Heat stress (X) and Drought stress (Y) SE ASUME QUE ESTE EXISTE Y SE CALCULO ANTES
  tst <- data.frame(bi_class = sort(unique(tbl$bi_class)))
  tst$col <- pals::stevens.bluered(n = nrow(tst))
  tbl <- dplyr::left_join(x = tbl, y = tst, by = 'bi_class')
} else {
  # Obtain waterlogging vs heat
  tbl <- biscale::bi_class(.data = tbl, x = Heat, y = Waterlogging, style = "quantile", dim = 3) %>%
    mutate(ht_wl = ifelse(str_detect(bi_class, "NA"), NA, bi_class))
  # Cross table between Heat stress (X) and Waterlogging stress (Y)
  tst <- data.frame(bi_class = sort(unique(tbl$ht_wl)))
  tst$col <- pals::stevens.bluered(n = nrow(tst))
  tbl <- dplyr::left_join(x = tbl, y = tst, by = c('ht_wl' = 'bi_class'))
}

# Country shapefile
cnt <- sf::read_sf(paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_gadm/',country,'_GADM0.shp'))
# Regions of study
rgs <- sf::read_sf(paste0(root,'/1.Data/shps/',tolower(country),'/',tolower(iso),'_regions/',tolower(iso),'_regions.shp'))

# Add nice labels
tbl <- tbl %>%
  dplyr::mutate(period = ifelse(time1 == 1,
                                'Historic',
                                ifelse(time1 == 2,
                                       '2021-2040',
                                       '2041-2060'))) %>%
  dplyr::mutate(period = factor(period, levels = c('Historic','2021-2040','2041-2060')))

# Hazards counts maps
categories <- c('Drought','Heat','Waterlogging')
categories %>%
  purrr::map(.f = function(category){
    cnt %>%
      ggplot2::ggplot() +
      ggplot2::geom_sf(fill = NA, color = gray(.5)) +
      ggplot2::geom_tile(data = tbl, aes_string(x = 'x', y = 'y', fill = factor(category)), alpha = 1) +
      ggplot2::scale_fill_brewer(palette = 'PuBuGn') +
      ggplot2::coord_equal() +
      ggplot2::theme_bw() +
      ggplot2::xlab('') +
      ggplot2::ylab('') +
      ggplot2::facet_wrap(~period) +
      ggplot2::labs(fill = 'Count') +
      ggplot2::theme(axis.text        = element_blank(),
                     axis.ticks       = element_blank(),
                     strip.text.x     = element_text(size = 17),
                     strip.background = element_rect(colour = "black", fill = "white")) +
      ggplot2::geom_sf(data = rgs, fill = NA, color = 'black') +
      # ORGANIZAR DONDE SE VAN A GUARDAR LOS MAPAS
      ggplot2::ggsave(filename = 'D:/OneDrive - CGIAR/WFP-profiles/HTI_waterlogging.png', device = 'png', width = 14, height = 7, units = 'in', dpi = 300)
  })

# Bivariate choropleth map
# ESTA DEPENDE DE LA COMBINACION DE VARIABLES
cnt %>%
  ggplot2::ggplot() +
  ggplot2::geom_sf(fill = NA, color = gray(.5)) +
  ggplot2::geom_tile(data = tbl, aes(x = x, y = y), fill = tbl$col, alpha = 1) +
  ggplot2::coord_equal() +
  ggplot2::theme_bw() +
  ggplot2::xlab('') +
  ggplot2::ylab('') +
  ggplot2::facet_wrap(~period) +
  ggplot2::theme(axis.text        = element_blank(),
                 axis.ticks       = element_blank(),
                 strip.text.x     = element_text(size = 17),
                 strip.background = element_rect(colour = "black", fill = "white")) +
  ggplot2::geom_sf(data = rgs, fill = NA, color = 'black') +
  # ORGANIZAR DONDE SE VAN A GUARDAR LOS MAPAS
  ggplot2::ggsave(filename = 'D:/OneDrive - CGIAR/WFP-profiles/HTI_drought_vs_heat.png', device = 'png', width = 14, height = 7, units = 'in', dpi = 300)

# Bivariate choropleth map heat vs waterlogging
cnt %>%
  ggplot2::ggplot() +
  ggplot2::geom_sf(fill = NA, color = gray(.5)) +
  ggplot2::geom_tile(data = mapbivar, aes(x = x, y = y), fill = mapbivar$col.y, alpha = 1) +
  ggplot2::coord_equal() +
  ggplot2::theme_bw() +
  ggplot2::xlab('') +
  ggplot2::ylab('') +
  ggplot2::facet_wrap(~period) +
  ggplot2::theme(axis.text        = element_blank(),
                 axis.ticks       = element_blank(),
                 strip.text.x     = element_text(size = 17),
                 strip.background = element_rect(colour = "black", fill = "white")) +
  ggplot2::geom_sf(data = rgs, fill = NA, color = 'black') +
  # ORGANIZAR DONDE SE VAN A GUARDAR LOS MAPAS
  ggplot2::ggsave(filename = 'D:/OneDrive - CGIAR/WFP-profiles/HTI_heat_vs_waterlogging.png', device = 'png', width = 14, height = 7, units = 'in', dpi = 300)

# Legend box
tst$x <- as.numeric(substr(x = tst$bi_class, start=1, stop=1))
tst$y <- as.numeric(substr(x = tst$bi_class, start=3, stop=3))
tst %>%
  ggplot2::ggplot(aes(x = x, y = y)) +
  ggplot2::geom_tile(fill = tst$col, alpha = 1) +
  ggplot2::coord_equal() +
  ggplot2::theme_minimal() +
  ggplot2::xlab('') +
  ggplot2::ylab('') +
  ggplot2::theme(axis.text       = element_text(size = 35),
                 axis.title      = element_text(size = 20),
                 legend.text     = element_text(size = 17),
                 legend.title    = element_blank(),
                 plot.title      = element_text(size = 25),
                 plot.subtitle   = element_text(size = 17),
                 strip.text.x    = element_text(size = 17),
                 plot.caption    = element_text(size = 15, hjust = 0),
                 legend.position = "bottom") +
  # ORGANIZAR DONDE SE VAN A GUARDAR ESTE CUADRITO
  ggplot2::ggsave(filename = 'D:/OneDrive - CGIAR/WFP-profiles/legend_box.png', device = 'png', width = 5, height = 5, units = 'in', dpi = 300)
