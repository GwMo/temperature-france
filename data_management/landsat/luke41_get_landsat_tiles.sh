#!/usr/bin/env bash

# Copy Landsat 7 data from /scratch_r730/landsat/ to the current dir
# This script must be executed on luke41

source_dir="/scratch_r730/landsat/landsat-7/geographic/brightness_ndvi/"
dest_dir=$(pwd)

if [[ $# -lt 1 ]]; then
  echo "You must specify at least one WRS-2 Path/Row e.g. get_landsat_tiles.sh '194029'"
  exit 1
fi

regexp_base="^LE07"
regexps=()
for rowpath in "$@"; do
  if [[ $rowpath =~ ^[0-9]{6}$ ]]; then
    regexps+=("$regexp_base$rowpath")
  else
    echo "Not a valid WRS-2 Path/Row: $rowpath"
    exit 1
  fi
done

tiles=()
for regexp in ${regexps[@]}; do
  # Quote with space at end or else tile names from sequential regexps will be mashed together
  # e.g. LE07194029xxx.tar.gzLE07194020xxx.tar.gz
  tiles+="$(ls $source_dir | egrep $regexp) "
done

read -p "Found $(echo ${tiles[*]} | wc -w) matching tiles. Copy to $(pwd) ? (y/n) " yn
if [[ $yn == 'y' ]]; then
  for tile in ${tiles[@]}; do
    cp -nv "$source_dir$tile" .
  done
else
  echo "No tiles copied"
fi
