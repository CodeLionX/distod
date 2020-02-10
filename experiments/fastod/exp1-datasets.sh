#!/usr/bin/env bash

declare -a datasets=( "hepatitis-sub.csv" ) #test-sub.csv, adult-sub.csv" )
declare -a delimiters=( "," ) #;" )
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
  echo "Running FASTOD on dataset ${dataset}"

  # fastod arguments: dataset csv_delimiter has_header
  /usr/bin/java -Xms30G -Xmx30G -jar fastod.jar "../data/${dataset}" "${delimiter}" "false" | tee "${logfile}"

  echo "Gathering results for dataset ${dataset}"
  {
    echo -n "${dataset},"
    grep "Run Time (ms)" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
    echo -n ","
    grep "# FD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'
    echo -n ","
    grep "# OD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
  } >>"${resultfile}"
done

# release lock file
rm -f /var/lock/fastod-exp1-datasets.lock
