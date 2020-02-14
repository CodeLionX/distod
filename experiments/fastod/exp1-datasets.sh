#!/usr/bin/env bash

declare -a datasets=( "test-sub.csv" "chess-sub.csv" "letter-sub.csv" "hepatitis-sub.csv" "adult-sub.csv" "fd-reduced-1k-30-sub.csv" "flight_1k_30c-sub.csv" "plista-sub.csv" "ncvoter-1m-19-sub.csv" )
declare -a delimiters=( "," "," "," "," ";" "," ";" ";" "," )

resultfolder="results"
resultfile="${resultfolder}/metrics.csv"

# write lock file
touch /var/lock/fastod-exp1-datasets.lock

mkdir -p "${resultfolder}"
echo "Dataset,Runtime (ms),#FDs,#ODs" >"${resultfile}"

for (( i=0; i<${#datasets[@]}; ++i )); do
  dataset="${datasets[i]}"
  delimiter="${delimiters[i]}"
  logfile="${resultfolder}/${dataset}.log"

  echo ""
  echo ""
  echo "Running FASTOD on dataset ${dataset} and delimiter ${delimiter}"

  # fastod arguments: dataset csv_delimiter has_header
  /usr/bin/java -Xms60G -Xmx60G -jar fastod.jar "../data/${dataset}" "${delimiter}" "false" 2>&1 | tee "${logfile}"

  echo "Gathering results for dataset ${dataset}"
  {
    echo -n "${dataset},"
    grep "Run Time (ms)" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
    echo -n ","
    grep "# FD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'
    echo -n ","
    grep "# OD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'
    # force newline
    echo ""
  } >>"${resultfile}"
done

# release lock file
rm -f /var/lock/fastod-exp1-datasets.lock
