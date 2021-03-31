#!/usr/bin/env bash

datasets="bridges-sub.json imdb-sub.json dblp-sub.json tpch-sub.json"
resultfolder="results"
resultfile="${resultfolder}/metrics.csv"
N=1

# write lock file
touch /var/lock/fastod-spark-exp1-datasets.lock

mkdir -p "${resultfolder}"
echo "Dataset,Run,Runtime (s),#FDs,#ODs  (misses double ODs!)" >"${resultfile}"

for dataset in ${datasets}; do
  echo ""
  echo ""
  echo "Running distributed FASTOD (Spark) on dataset ${dataset}"

  for (( n=0; n<N; ++n )); do
    logfile="${resultfolder}/${dataset}-${n}.log"
    echo ""
    echo "Run ${n}"

    timeout --signal=15 24h \
      /opt/spark/2.4.4/bin/spark-submit --jars libs/fastutil-6.1.0.jar,libs/lucene-core-4.5.1.jar \
        --class FastODMain \
        --master spark://odin01:7077 \
        --driver-memory 31G \
        --executor-memory 28G \
        --num-executors 11 \
        --executor-cores 20 \
        --total-executor-cores 220 \
        distributed-fastod.jar "file:$(pwd)/data/${dataset}" "100" 2>&1 | tee "${logfile}"
    was_killed=$(( $? == 124 ))


    echo "Gathering results for dataset ${dataset}"
    {
      echo -n "${dataset},${n},"
      grep "==== Total" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
      echo -n ","
      grep "# FD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'
      echo -n ","
      grep "# OD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'
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
rm -f /var/lock/fastod-spark-exp1-datasets.lock
