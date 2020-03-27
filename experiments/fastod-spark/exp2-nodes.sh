#!/usr/bin/env bash

datasets="letter-sub.json"
nodes="1 2 3 4 5 6 7 8 9 10 11"
resultfolder="results"
resultfile="${resultfolder}/metrics.csv"

# write lock file
touch /var/lock/fastod-spark-exp2-nodes.lock

mkdir -p "${resultfolder}"
echo "Dataset,#Nodes,Runtime (s)" >"${resultfile}"

for dataset in ${datasets}; do
  for n in ${nodes}; do
    logfile="${resultfolder}/${dataset}-${n}nodes.log"

    echo ""
    echo ""
    echo "Running distributed FASTOD (Spark) on dataset ${dataset} with ${n}  nodes"

    e_cores=20
    total_e_cores=$(( n * e_cores ))
    timeout --signal=15 24h \
      /opt/spark/2.4.4/bin/spark-submit --jars libs/fastutil-6.1.0.jar,libs/lucene-core-4.5.1.jar \
        --class FastODMain \
        --master spark://odin01:7077 \
        --driver-memory 31G \
        --executor-memory 28G \
        --num-executors "${n}" \
        --executor-cores "${e_cores}" \
        --total-executor-cores "${total_e_cores}" \
        distributed-fastod.jar "file:$(pwd)/data/${dataset}" "100" 2>&1 | tee "${logfile}"


    echo "Gathering results for experiment with ${n} nodes on ${dataset}"
    {
      echo -n "${dataset},"
      echo -n "${n},"
      grep "==== Total" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
      # force newline
      echo ""
    } >>"${resultfile}"
  done
done

# release lock file
rm -f /var/lock/fastod-spark-exp2-nodes.lock
