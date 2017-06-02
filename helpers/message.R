# Miscelanneous functions used in other scripts

# Display messages with a timestamp
display <- function(msg) {
  paste(Sys.time(), msg, sep = ": ") %>% message
}
