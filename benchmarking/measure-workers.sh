#!/usr/bin/env bash

worker_configurations="1 2 3 4"

for workers in ${worker_configurations}; do
  filename="bench-results-${workers}workers.txt"
  echo "Benchmark of DISTOD with ${workers} workers" >"${filename}"

  for i in {0..10}; do
    result=$(
      java -Xmx8g -Xms8g \
        -Dconfig.file=bench.conf \
        -Dlogback.configurationFile=logback.xml \
        -Ddistod.max-workers="${workers}" \
        -jar distod.jar
    )

    echo "Test ${i}:" >>"${filename}"
    echo "${result}" >>"${filename}"
  done
done
