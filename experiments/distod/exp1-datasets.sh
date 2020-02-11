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
  mkdir -p "${resultfolder}/${dataset}"

  echo ""
  echo ""
  echo "Running DISTOD on dataset ${dataset}"

  # start followers
  for node in ${nodes}; do
    ssh "${node}" "cd ~/distod && screen -d -S \"distod-exp1-datasets\" -m ./start.sh"
  done

  t0=$(date +%s)

  # start leader
  java -Xms60g -Xmx60g -XX:+UseG1GC \
    -Dconfig.file="$(hostname).conf" \
    -Dlogback.configurationFile=logback.xml \
    -Ddistod.input.path="../data/${dataset}" \
    -Ddistod.input.has-header="no" \
    -jar distod.jar 2>&1 | tee "${logfile}"

  t1=$(date +%s)
  duration=$(( t1 - t0 ))

  running_nodes=""
  for node in ${nodes}; do
    ssh "${node}" screen -ls distod-exp1-datasets >/dev/null
    if [ $? == 1 ]; then
      running_nodes="yes"
      break
    fi
  done

  if [[ $duration -lt 30 && "${running_nodes}" != "" ]]; then
    echo "Waiting till followers stopped, before force killing them."
    sleep $(( 30 - duration ))
  fi

  # collect results
  echo "Killing still running nodes and gathering results for dataset ${dataset}"
  mv distod.log "${resultfolder}/${dataset}/distod-odin01.log"
  for node in ${nodes}; do
    ssh "${node}" screen -ls distod-exp1-datasets >/dev/null
    # still running --> kill follower
    if [ $? == 1 ]; then
      echo "Killing follower on node ${node}"
      ssh "${node}" screen -S distod-exp1-datasets -X quit
    fi
    scp "${node}":~/distod/distod.log "${resultfolder}/${dataset}/distod-${node}.log" >/dev/null
    # intentially put argument in single quotes to let the target shell expand the ~
    ssh "${node}" rm -f '~/distod/distod.log'
  done

  {
    echo -n "${dataset},"
    grep "TIME Overall runtime" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
    echo -n ","
    grep "# FD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'
    echo -n ","
    grep "# OD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'
    # force newline
    echo ""
  } >>"${resultfile}"

  # synchronize and wait a bit
  sleep 2
  wait
done

# release lock file
rm -f /var/lock/distod-exp1-datasets.lock
