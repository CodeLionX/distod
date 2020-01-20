#!/usr/bin/env bash

# do for all files in folder (fish):
#
# for f in ./*
#     ../scripts/print-results-csv.sh "$f"
# end

file=$1

if [ -z "$file" ]; then
  printf "Argument not set! Usage:\n %s path/to/bench-results.txt" "$0"
  exit 1

else
  target="$file.csv"
  echo "Overall runtime,Data Loading,State Initialization,Dequeueing,State Update,State Updates,Partition Job,Partition Generation,Partition Mgmt,Split Checks,Swap Checks" >"$target"

  declare -a column1 column2 column3 column4 column5 column7 column8 column10 column11
  mapfile -t column1 < <(grep "Overall" "$file" | cut -d ':' -f2 | cut -d ' ' -f2)
  mapfile -t column2 < <(grep "loading" "$file" | cut -d ':' -f2 | cut -d ' ' -f2)
  mapfile -t column3 < <(grep "initialization" "$file" | cut -d ':' -f2 | cut -d ' ' -f2)
  mapfile -t column4 < <(grep "Dequeue" "$file" | cut -d ':' -f2 | cut -d ' ' -f2)
  mapfile -t column5 < <(grep "State update:" "$file" | cut -d ':' -f2 | cut -d ' ' -f2)
  mapfile -t column7 < <(grep "job" "$file" | cut -d ':' -f2 | cut -d ' ' -f2)
  mapfile -t column8 < <(grep "Partition generation" "$file" | cut -d ':' -f2 | cut -d ' ' -f2)
  mapfile -t column10 < <(grep "Split" "$file" | cut -d ':' -f2 | cut -d ' ' -f2)
  mapfile -t column11 < <(grep "Swap" "$file" | cut -d ':' -f2 | cut -d ' ' -f2)

  for ((i = 0; i <= 5; ++i)); do
    echo "${column1[$i]},${column2[$i]},${column3[$i]},${column4[$i]},${column5[$i]},,${column7[$i]},${column8[$i]},,${column10[$i]},${column11[$i]}" >>"$target"
  done
fi
