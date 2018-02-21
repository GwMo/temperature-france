data_dir="$(readlink -f ~/data/meteo_france/sim/original)"
output_dir="$(readlink -f ~/data/meteo_france/sim/annual)"
mkdir -p $output_dir

# Matches SIM2_YYYY_YYYY[MM].csv
infile_regexp="^SIM2_([0-9]{4})_([0-9]{4})([0-9]{2})?.csv$"

# Process all files in the data dir matching the infile regexp
for infile in $(ls $data_dir); do
  if [[ $infile =~ $infile_regexp ]]; then
    echo "Processing $infile"

    # Get the absolute path to the data file
    infile="$data_dir/$infile"

    # Get start and end years from the regexp captures
    start_year="${BASH_REMATCH[1]}"
    end_year="${BASH_REMATCH[2]}"

    # Extract data for each year to a separate file
    for year in $(seq $start_year 1 $end_year); do
      echo "  Extracting data for $year"

      outfile="$output_dir/SIM2_$year.csv"
      line_regexp="^[0-9]+;[0-9]+;$year"

      # Echo the header to the output file
      head -n 1 $infile > $outfile

      # Append data for the year to the output file
      grep -P $line_regexp $infile >> $outfile
    done
  fi
done
