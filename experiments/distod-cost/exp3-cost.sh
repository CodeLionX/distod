#!/usr/bin/env bash

datasets="hepatitis-sub.csv adult-sub.csv"
parallelism="20 18 16 14 12 10 9 8 7 6 5 4 3 2 1"
resultfolder="results"
resultfile="${resultfolder}/metrics.csv"

# write lock file
touch /var/lock/distod-exp3-cost.lock

mkdir -p "${resultfolder}"
echo "Dataset,Parallelism,Runtime (ms)" >"${resultfile}"

for dataset in ${datasets}; do
  for n in ${parallelism}; do
    mkdir -p "${resultfolder}/${dataset}-p${n}"
    logfile="${resultfolder}/${dataset}-p${n}/out.log"

    echo ""
    echo ""
    echo "Running DISTOD with parallelism ${n} on dataset ${dataset}"

    t0=$(date +%s)

    # start distod
    timeout --signal=15 2h \
      java -Xms28g -Xmx28g -XX:+UseG1GC -XX:G1ReservePercent=10 \
        -XX:MaxGCPauseMillis=400 -XX:G1HeapWastePercent=1 \
        -XX:+UnlockExperimentalVMOptions -XX:G1MixedGCLiveThresholdPercent=60 \
        -XX:G1MixedGCCountTarget=10 -XX:G1OldCSetRegionThresholdPercent=20 \
        -Dconfig.file=application.conf \
        -Dlogback.configurationFile=logback.xml \
        -Ddistod.input.path="../data/${dataset}" \
        -Ddistod.input.has-header="no" \
        -Ddistod.max-parallelism="${n}" \
        -jar distod.jar 2>&1 | tee "${logfile}"

    t1=$(date +%s)
    duration=$(( t1 - t0 ))
    echo "Duration: ${duration} s"

    # collect results
    echo "Collecting results for dataset ${dataset} with parallelism ${n}"
    mv distod.log "${resultfolder}/${dataset}-p${n}/"
    mv results.txt "${resultfolder}/${dataset}-p${n}/"

    {
      echo -n "${dataset},"
      echo -n "${n},"
      grep "TIME Overall runtime" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
      # force newline
      echo ""
    } >>"${resultfile}"

    # wait a bit
    sleep 2
  done
done

# release lock file
rm -f /var/lock/distod-exp3-cost.lock
