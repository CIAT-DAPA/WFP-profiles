library(tidyverse)
library(pals)

d <- 0:1 # Drought stress
h <- 0:2 # Heat stress
w <- 0:5 # Waterlogging

# Cross table between Heat stress (X) and Drought stress (Y)
tst <- data.frame(expand.grid(h,d))
tst$col <- pals::stevens.bluered(n = nrow(tst))

tst %>% ggplot(aes(x = Var1, y = Var2)) +
  geom_tile(fill = tst$col) +
  scale_fill_identity() +
  coord_equal()
