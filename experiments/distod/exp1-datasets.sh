#!/usr/bin/env bash

datasets="test-sub.csv chess-sub.csv plista-sub.csv letter-sub.csv hepatitis-sub.csv adult-sub.csv fd-reduced-1k-30-sub.csv flight_1k_30c-sub.csv ncvoter-1m-19-sub.csv"
resultfolder="results"
resultfile="${resultfolder}/metrics.csv"

nodes="thor01 thor02 thor03 thor04 odin02 odin03 odin04 odin05 odin06 odin07 odin08"

# write lock file
touch /var/lock/distod-exp1-datasets.lock

mkdir -p "${resultfolder}"
echo "Dataset,Runtime (ms),#FDs,#ODs" >"${resultfile}"

for dataset in ${datasets}; do
  logfile="${resultfolder}/${dataset}/out.log"

  echo ""
  echo ""
  echo "Running DISTOD on dataset ${dataset}"

  # start followers
  for node in ${nodes}; do
    ssh "${node}" "cd ~/distod && screen -d -S \"distod-exp1-datasets\" -m ./start.sh"
  done

  # start leader
  java -Xms60g -Xmx60g -XX:+UseG1GC \
    -Dconfig.file="$(hostname).conf" \
    -Dlogback.configurationFile=logback.xml \
    -Ddistod.input.path="../data/${dataset}" \
    -Ddistod.input.has-header="no" \
    -jar distod.jar 2>&1 | tee "${logfile}"

  echo "Gathering results for dataset ${dataset}"

  # collect results
  mv distod.log "${resultfolder}/${dataset}/distod-odin01.log"
  for node in ${nodes}; do
    ssh "${node}" screen -ls distod-exp1-datasets
    # still running --> kill follower
    if [ $? == 1 ]; then
      echo "Killing follower on node ${node}"
      ssh "${node}" screen -S distod-exp1-datasets -X quit
    fi
    scp "${node}":~/distod/distod.log "${resultfolder}/${dataset}/distod-${node}.log" &
  done

  wait

  {
    echo -n "${dataset},"
    grep "TIME Overall runtime" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
    echo -n ","
    grep "# FD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'
    echo -n ","
    grep "# OD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
  } >>"${resultfile}"
done

# release lock file
rm -f /var/lock/distod-exp1-datasets.lock
