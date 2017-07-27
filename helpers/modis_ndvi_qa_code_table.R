# Return a table to aid in the interpretation of MODIS LST QA codes
# Inspired by https://stevemosher.wordpress.com/2012/12/05/modis-qc-bits/
modis_ndvi_qa_code_table <- function() {
  # List all possible QA codes and their 16-bit big-endian binary notation
  qa_codes <- lapply(0:65535, function(val) {
    intToBits(val) %>% as.integer %>% .[16:1] %>% c(val, .)
  }) %>% do.call(rbind.data.frame, .)
  names(qa_codes) <- paste0("bit", 15:0) %>% c("code", .)

  # Label the overall quality indicated by the QA bit values
  qa_codes$quality[qa_codes$bit1 == 0 & qa_codes$bit0 == 0] <- "Good"     # Good quality, no need to check details
  qa_codes$quality[qa_codes$bit1 == 0 & qa_codes$bit0 == 1] <- "Marginal" # Lower quality, see other bits for details
  qa_codes$quality[qa_codes$bit1 == 1 & qa_codes$bit0 == 0] <- "Cloud"    # Pixel probably cloudy
  qa_codes$quality[qa_codes$bit1 == 1 & qa_codes$bit0 == 1] <- "None"     # Pixel not produced for reason other than clouds

  # Label the usefulness indicated by the QA bit values
  # 1 = highest quality, 13 = lowest quality, NA = not useful
  qa_codes$usefulness[qa_codes$bit5 == 0 & qa_codes$bit4 == 0 & qa_codes$bit3 == 0 & qa_codes$bit2 == 0] <-  1
  qa_codes$usefulness[qa_codes$bit5 == 0 & qa_codes$bit4 == 0 & qa_codes$bit3 == 0 & qa_codes$bit2 == 1] <-  2
  qa_codes$usefulness[qa_codes$bit5 == 0 & qa_codes$bit4 == 0 & qa_codes$bit3 == 1 & qa_codes$bit2 == 0] <-  3
  qa_codes$usefulness[qa_codes$bit5 == 0 & qa_codes$bit4 == 0 & qa_codes$bit3 == 1 & qa_codes$bit2 == 1] <-  4
  qa_codes$usefulness[qa_codes$bit5 == 0 & qa_codes$bit4 == 1 & qa_codes$bit3 == 0 & qa_codes$bit2 == 0] <-  5
  qa_codes$usefulness[qa_codes$bit5 == 0 & qa_codes$bit4 == 1 & qa_codes$bit3 == 0 & qa_codes$bit2 == 1] <-  6
  qa_codes$usefulness[qa_codes$bit5 == 0 & qa_codes$bit4 == 1 & qa_codes$bit3 == 1 & qa_codes$bit2 == 0] <-  7
  qa_codes$usefulness[qa_codes$bit5 == 0 & qa_codes$bit4 == 1 & qa_codes$bit3 == 1 & qa_codes$bit2 == 1] <-  8
  qa_codes$usefulness[qa_codes$bit5 == 1 & qa_codes$bit4 == 0 & qa_codes$bit3 == 0 & qa_codes$bit2 == 0] <-  9
  qa_codes$usefulness[qa_codes$bit5 == 1 & qa_codes$bit4 == 0 & qa_codes$bit3 == 0 & qa_codes$bit2 == 1] <- 10
  qa_codes$usefulness[qa_codes$bit5 == 1 & qa_codes$bit4 == 0 & qa_codes$bit3 == 1 & qa_codes$bit2 == 0] <- 11
  qa_codes$usefulness[qa_codes$bit5 == 1 & qa_codes$bit4 == 0 & qa_codes$bit3 == 1 & qa_codes$bit2 == 1] <- 12
  qa_codes$usefulness[qa_codes$bit5 == 1 & qa_codes$bit4 == 1 & qa_codes$bit3 == 0 & qa_codes$bit2 == 0] <- 13
  qa_codes$usefulness[qa_codes$bit5 == 1 & qa_codes$bit4 == 1 & qa_codes$bit3 == 0 & qa_codes$bit2 == 1] <- NA
  qa_codes$usefulness[qa_codes$bit5 == 1 & qa_codes$bit4 == 1 & qa_codes$bit3 == 1 & qa_codes$bit2 == 0] <- NA
  qa_codes$usefulness[qa_codes$bit5 == 1 & qa_codes$bit4 == 1 & qa_codes$bit3 == 1 & qa_codes$bit2 == 1] <- NA

  # Store usefulness as an integer to reduce filesize of extracted data
  qa_codes$usefulness <- as.integer(qa_codes$usefulness)

  # QA bits 6-15 not currently used

  # Clear the usefulness of pixels that were not produced
  qa_codes$usefulness[qa_codes$quality == "None"] <- NA

  # Note that many of the QA codes are likely never used. For example the QA
  # code 2 (000010) means the pixel is most likely cloudy but is of very high
  # quality. Cloudiness results in lower quality pixels so this code is probably
  # never used. Similarly the QA code 27 (011011) means the pixel was not
  # produced but is of medium usefulness. This is clearly nonsensical so we
  # clear the usefulness but keep the code just in case.

  # Return the table
  qa_codes
}

#  code bit5 bit4 bit3 bit2 bit1 bit0  quality usefulness
#     0    0    0    0    0    0    0     Good          1
#     1    0    0    0    0    0    1 Marginal          1
#     2    0    0    0    0    1    0    Cloud          1
#     3    0    0    0    0    1    1     None         NA
# ...
# 65532    1    1    1    1    0    0     Good         NA
# 65533    1    1    1    1    0    1 Marginal         NA
# 65534    1    1    1    1    1    0    Cloud         NA
# 65535    1    1    1    1    1    1     None         NA
