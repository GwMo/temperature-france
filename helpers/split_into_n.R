# Split something into a list of n approximately equal groups
split_into_n <- function(x, n) {
  lngth <- length(x)
  split(x, ceiling(1:lngth / (lngth / n)))
}
