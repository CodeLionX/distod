#!/usr/bin/env bash

datasets="test-sub.json" #adult-sub.json"
resultfolder="results"
resultfile="${resultfolder}/metrics.csv"

# write lock file
touch /var/lock/fastod-spark-exp1-datasets.lock

mkdir -p "${resultfolder}"
echo "Dataset,Runtime (s),#FDs,#ODs" >"${resultfile}"

for dataset in ${datasets}; do
  logfile="${resultfolder}/${dataset}.log"

  echo ""
  echo ""
  echo "Running distributed FASTOD (Spark) on dataset ${dataset}"

  /opt/spark/2.4.4/bin/spark-submit --jars libs/fastutil-6.1.0.jar,libs/lucene-core-4.5.1.jar \
    --class FastODMain \
    --master spark://odin01:7077 \
    --executor-memory 30G \
    --num-executors 11 \
    --executor-cores 20 \
    --total-executor-cores 220 \
    distributed-fastod.jar "file:$(pwd)/data/${dataset}" "100" | tee "${logfile}"


  echo "Gathering results for dataset ${dataset}"
  {
    echo -n "${dataset},"
    grep "==== Total" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
    echo -n ","
    grep "# FD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'
    echo -n ","
    grep "# OD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
  } >>"${resultfile}"
done

# release lock file
rm -f /var/lock/fastod-spark-exp1-datasets.lock
