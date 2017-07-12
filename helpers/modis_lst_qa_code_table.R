# Return a table to aid in the interpretation of MODIS LST QA codes
# Inspired by https://stevemosher.wordpress.com/2012/12/05/modis-qc-bits/
modis_lst_qa_code_table <- function() {
  # List all possible QA codes and their 8-bit big-endian binary notation
  qa_codes <- lapply(0:255, function(val) {
    intToBits(val) %>% as.integer %>% .[8:1] %>% c(val, .)
  }) %>% do.call(rbind, .) %>% as.data.frame
  names(qa_codes) <- paste0("bit", 7:0) %>% c("code", .)

  # Label the overall quality indicated by the QA bit values
  qa_codes$quality[qa_codes$bit1 == 0 & qa_codes$bit0 == 0] <- "Good"     # Good quality, no need to check details
  qa_codes$quality[qa_codes$bit1 == 0 & qa_codes$bit0 == 1] <- "Marginal" # Lower quality, see other bits for details
  qa_codes$quality[qa_codes$bit1 == 1 & qa_codes$bit0 == 0] <- "Cloud"    # Pixel not produced due to cloud effects
  qa_codes$quality[qa_codes$bit1 == 1 & qa_codes$bit0 == 1] <- "None"     # Pixel not produced for reason other than cloud

  # QA bits 2 and 3 do not add any useful information
  # (good data quality / other quality data / TBD / TBD)

  # Label the emissivity error indicated by the QA bit values
  qa_codes$emis_error[qa_codes$bit5 == 0 & qa_codes$bit4 == 0] <- "<= 0.01"
  qa_codes$emis_error[qa_codes$bit5 == 0 & qa_codes$bit4 == 1] <- "<= 0.02"
  qa_codes$emis_error[qa_codes$bit5 == 1 & qa_codes$bit4 == 0] <- "<= 0.04"
  qa_codes$emis_error[qa_codes$bit5 == 1 & qa_codes$bit4 == 1] <- "> 0.04"

  # Label the LST error indicated by the QA bit values
  qa_codes$lst_error[qa_codes$bit7 == 0 & qa_codes$bit6 == 0] <- "<= 1K"
  qa_codes$lst_error[qa_codes$bit7 == 0 & qa_codes$bit6 == 1] <- "<= 2K"
  qa_codes$lst_error[qa_codes$bit7 == 1 & qa_codes$bit6 == 0] <- "<= 3K"
  qa_codes$lst_error[qa_codes$bit7 == 1 & qa_codes$bit6 == 1] <- "> 3K"

  # Clear some nonsensical error values
  # Errors are nonsensical if the pixel was not produced due to cloud or other
  qa_codes$emis_error[qa_codes$quality %in% c("Cloud", "None")] <- NA
  qa_codes$lst_error[qa_codes$quality %in% c("Cloud", "None")] <- NA

  # Note that many of the QA codes are likely never used. For example the QA
  # code 1 (00000001) means marginal quality but minimal emissivity and LST
  # error. Minimal error corresponds to good quality so this code is probably
  # never used. Similarly the QA code 6 (00000101) means the pixel was not
  # produced due to cloud but has minimal emissivity and LST error. This is
  # nonsensical so we clear the error values but keep the code just in case.

  # Return the table
  qa_codes
}

# code bit7 bit6 bit5 bit4 bit3 bit2 bit1 bit0  quality emis_error lst_error
#    0    0    0    0    0    0    0    0    0     Good    <= 0.01     <= 1K
#    1    0    0    0    0    0    0    0    1 Marginal    <= 0.01     <= 1K
#    2    0    0    0    0    0    0    1    0    Cloud       <NA>      <NA>
#    3    0    0    0    0    0    0    1    1     None       <NA>      <NA>
# ...
#  252    1    1    1    1    1    1    0    0     Good     > 0.04      > 3K
#  253    1    1    1    1    1    1    0    1 Marginal     > 0.04      > 3K
#  254    1    1    1    1    1    1    1    0    Cloud       <NA>      <NA>
#  255    1    1    1    1    1    1    1    1     None       <NA>      <NA>
