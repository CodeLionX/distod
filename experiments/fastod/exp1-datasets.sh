#!/usr/bin/env bash

declare -a datasets=( "test-sub.csv" "iris-sub.csv" "chess-sub.csv" "abalone-sub.csv" "bridges-sub.csv" "adult-sub.csv" "letter-sub.csv" "hepatitis-sub.csv" "flight_1k_30c-sub.csv" "fd-reduced-250k-30-sub.csv" "horse-sub.csv" "plista-sub.csv" "ncvoter-1m-19-sub.csv" )
declare -a delimiters=( "," "," "," "," "," ";" "," "," ";" "," ";" ";" "," )

resultfolder="results"
resultfile="${resultfolder}/metrics.csv"
N=3

# write lock file
touch /var/lock/fastod-exp1-datasets.lock

mkdir -p "${resultfolder}"
echo "Dataset,Run,Runtime (ms),#FDs,#ODs" >"${resultfile}"

for (( i=0; i<${#datasets[@]}; ++i )); do
  dataset="${datasets[i]}"
  delimiter="${delimiters[i]}"
  echo ""
  echo ""
  echo "Running FASTOD on dataset ${dataset} and delimiter ${delimiter}"

  for (( n=0; n<N; ++n )); do
    logfile="${resultfolder}/${dataset}-${n}.log"
    echo ""
    echo "Run ${n}"

    # fastod arguments: dataset csv_delimiter has_header
    timeout --signal=15 24h \
      /usr/bin/java -Xms31G -Xmx31G -jar fastod.jar "../data/${dataset}" "${delimiter}" "false" 2>&1 | tee "${logfile}"
    was_killed=$(( $? == 124 ))

    echo "Gathering results for dataset ${dataset}"
    fds=$(grep -c "FD" results.txt)
    ods=$(grep -c "OD" results.txt)
    {
      echo -n "${dataset},${n},"
      grep "Run Time (ms)" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
      echo -n ",${fds},${ods}"
      # force newline
      echo ""
    } >>"${resultfile}"

    # do not perform other runs if it hit the timelimit
    if (( was_killed )); then
      echo "Run exceeded timelimit, continuing with next dataset..."
      break
    fi
  done

  # calculate min for the runtime
  min_runtime=$( grep "${dataset}" "${resultfile}" | cut -d ',' -f3 | sort -n | head -n 1 )
  echo "${dataset},ALL,${min_runtime},," >>"${resultfile}"
done

# release lock file
rm -f /var/lock/fastod-exp1-datasets.lock
