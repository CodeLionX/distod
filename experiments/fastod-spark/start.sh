#!/usr/bin/env bash

dataset="letter-sub.json"
memory="28G"
logfile="fastod-spark-${dataset}.log"

/opt/spark/2.4.4/bin/spark-submit \
  --jars libs/fastutil-6.1.0.jar,libs/lucene-core-4.5.1.jar \
  --class FastODMain \
  --master spark://odin01:7077 \
  --driver-memory "${memory}" \
  --executor-memory "${memory}" \
  --num-executors 11 \
  --executor-cores 20 \
  --total-executor-cores 220 \
  distributed-fastod.jar "file:$(pwd)/../fastod-spark/data/${dataset}" "100" 2>&1 | tee "${logfile}"
