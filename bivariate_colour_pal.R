# -------------------------------------------------- #
# Climate Risk Profiles -- Hazard counts and bivariate choropleth map
# H. Achicanoy & A. Esquivel
# Alliance Bioversity-CIAT, 2021
# -------------------------------------------------- #

library(tidyverse)
library(pals)
library(fst)
library(raster)
library(terra)
library(RColorBrewer)

# Haiti (drought vs heat stress)
tbl <- fst::read_fst("//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/7.Results/Haiti/results/maps/all/Final.fst")

# Cross table between Heat stress (X) and Drought stress (Y)
tst <- data.frame(bi_class = sort(unique(tbl$bi_class)))
tst$col <- pals::stevens.bluered(n = nrow(tst))

tbl <- dplyr::left_join(x = tbl, y = tst, by = 'bi_class')

cnt <- sf::read_sf("//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/shps/haiti/hti_gadm/Haiti_GADM0.shp")
rgs <- sf::read_sf("//dapadfs.cgiarad.org/workspace_cluster_13/WFP_ClimateRiskPr/1.Data/shps/haiti/hti_regions/hti_regions.shp")

tbl <- tbl %>%
  dplyr::mutate(period = ifelse(time1 == 1,
                                'Historic',
                                ifelse(time1 == 2,
                                       '2021-2040',
                                       '2041-2060'))) %>%
  dplyr::mutate(period = factor(period, levels = c('Historic','2021-2040','2041-2060')))

# Hazards counts
cnt %>%
  ggplot2::ggplot() +
  ggplot2::geom_sf(fill = NA, color = gray(.5)) +
  ggplot2::geom_tile(data = tbl, aes(x = x, y = y, fill = factor(Waterlogging)), alpha = 1) +
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
  ggplot2::ggsave(filename = 'D:/OneDrive - CGIAR/WFP-profiles/HTI_waterlogging.png', device = 'png', width = 14, height = 7, units = 'in', dpi = 300)

# Bivariate choropleth map
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
  ggplot2::ggsave(filename = 'D:/OneDrive - CGIAR/WFP-profiles/HTI_drought_vs_heat.png', device = 'png', width = 14, height = 7, units = 'in', dpi = 300)

# Obtain waterlogging vs heat
mapbivar <- biscale::bi_class(.data = tbl, x = Heat, y = Waterlogging, style = "quantile", dim = 3) %>%
  mutate(ht_wl = ifelse(str_detect(bi_class, "NA"), NA, bi_class))
# Cross table between Heat stress (X) and Waterlogging stress (Y)
tst2 <- data.frame(bi_class = sort(unique(mapbivar$ht_wl)))
tst2$col <- pals::stevens.bluered(n = nrow(tst2))

mapbivar <- dplyr::left_join(x = mapbivar, y = tst2, by = c('ht_wl' = 'bi_class'))

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
  ggplot2::ggsave(filename = 'D:/OneDrive - CGIAR/WFP-profiles/legend_box.png', device = 'png', width = 5, height = 5, units = 'in', dpi = 300)
