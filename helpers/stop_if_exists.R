source("helpers/report.R")

# Stop script execution if the specified file exists
stop_if_exists <- function(path) {
  if (file.exists(path)) {
    report("Already exists:")
    report(out_path)

    # In an interactive session, stop but do not quit
    if (interactive()) {
      stop("File exists")
    }
    # In a non-interactive session, quit (easiest way to stop without raising an error)
    else {
      quit()
    }
  }
}
