# Display a message with a timestamp and also write it to the logfile, if one exists
report <- function(msg) {
  txt <- paste(Sys.time(), msg, sep = ": ")
  message(txt)
  if (exists("constants") && !is.null(constants$logfile)) {
    write(txt, constants$logfile, append = TRUE)
  }
}
