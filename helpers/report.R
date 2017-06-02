# Display a message with a timestamp
report <- function(msg) {
  paste(Sys.time(), msg, sep = ": ") %>% message
}
