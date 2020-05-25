#!/usr/bin/env bash

declare -a datasets=( "letter-sub.csv" )
declare -a delimiters=( "," )

resultfolder="results"
resultfile="${resultfolder}/metrics.csv"
N=1

declare -a memory_limits=( "28G" "8G" "4G" "2G" "1G" "512M" "256M" )

# write lock file
touch /var/lock/fastod-exp6-memory.lock

mkdir -p "${resultfolder}"
echo "Dataset,Memory Limit,Run,Runtime (ms),#FDs,#ODs" >"${resultfile}"

for (( i=0; i<${#datasets[@]}; ++i )); do
  dataset="${datasets[i]}"
  delimiter="${delimiters[i]}"
  echo ""
  echo ""
  echo "Running FASTOD on dataset ${dataset} and delimiter ${delimiter}"

  for (( j=0; j<${#memory_limits[@]}; ++j )); do
    memory_limit="${memory_limits[j]}"

    echo ""
    echo "Memory limit: ${memory_limit}"

    for (( n=0; n<N; ++n )); do
      logfile="${resultfolder}/${dataset}-${n}.log"
      echo ""
      echo "Run ${n}"

      # fastod arguments: dataset csv_delimiter has_header
      timeout --signal=15 24h \
        /usr/bin/java "-Xms${memory_limit}" "-Xmx${memory_limit}" \
        -jar fastod.jar "../data/${dataset}" "${delimiter}" "false" 2>&1 | tee "${logfile}"
      was_killed=$(( $? == 124 ))

      echo "Gathering results for dataset ${dataset}"
      fds=$(grep -c "FD" results.txt)
      ods=$(grep -c "OD" results.txt)
      {
        echo -n "${dataset},${memory_limit},${n},"
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
done

# release lock file
rm -f /var/lock/fastod-exp6-memory.lock
