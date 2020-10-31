#!/usr/bin/env bash

datasets="iris-sub.csv chess-sub.csv abalone-sub.csv bridges-sub.csv adult-sub.csv letter-sub.csv hepatitis-sub.csv flight_1k_30c-sub.csv fd-reduced-250k-30-sub.csv flight-500k-sub.csv"
resultfolder="results"
resultfile="${resultfolder}/metrics.csv"
declare -a master_dispatchers=(
  "akka.actor.default-dispatcher"
  "akka.actor.default-dispatcher"
  "akka.actor.default-dispatcher"
  "master-dispatcher"
  "master-dispatcher"
  "master-dispatcher"
)
declare -a worker_dispatchers=(
  "akka.actor.default-dispatcher"
  "thread-pool-dispatcher"
  "worker-affinity-dispatcher"
  "akka.actor.default-dispatcher"
  "thread-pool-dispatcher"
  "worker-affinity-dispatcher"
)

nodes="thor01 thor02 thor03 thor04 odin02 odin03 odin04 odin05 odin06 odin07 odin08"

# write lock file
touch /var/lock/distod-exp9-dispatchers.lock

mkdir -p "${resultfolder}"
echo "Dataset,Master Dispatcher, Worker Dispatcher,Runtime (ms),#FDs,#ODs" >"${resultfile}"

for dataset in ${datasets}; do

  for ((i = 0; i < ${#master_dispatchers[@]}; ++i)); do
    master_dispatcher="${master_dispatchers[i]}"
    worker_dispatcher="${worker_dispatchers[i]}"
    folder="${resultfolder}/${dataset}/${master_dispatcher}-${worker_dispatcher}"
    mkdir -p "${folder}"
    logfile="${folder}/out.log"
    echo ""
    echo ""
    echo "Running DISTOD on dataset ${dataset} with dispatchers: ${master_dispatcher}/${worker_dispatcher}"

    # start followers
    for node in ${nodes}; do
      ssh "${node}" "cd ~/distod && screen -d -S \"distod-exp9-dispatchers\" -m ./start.sh -Ddistod.master-pinned-dispatcher=\"${master_dispatcher}\" -Ddistod.cpu-bound-tasks-dispatcher=\"${worker_dispatcher}\""
    done

    t0=$(date +%s)

    # start leader
    heap_size=31g
    timeout --signal=15 24h \
      java "-Xms${heap_size}" "-Xmx${heap_size}" -XX:+UseG1GC -XX:G1ReservePercent=10 \
        -XX:MaxGCPauseMillis=400 -XX:G1HeapWastePercent=1 \
        -XX:+UnlockExperimentalVMOptions -XX:G1MixedGCLiveThresholdPercent=60 \
        -XX:G1MixedGCCountTarget=10 -XX:G1OldCSetRegionThresholdPercent=20 \
        -Dconfig.file="$(hostname).conf" \
        -Dlogback.configurationFile=logback.xml \
        -Ddistod.input.path="../data/${dataset}" \
        -Ddistod.input.has-header="no" \
        -Ddistod.master-pinned-dispatcher="${master_dispatcher}" \
        -Ddistod.cpu-bound-tasks-dispatcher="${worker_dispatcher}" \
        -Dfile.encoding=UTF-8 \
        -jar distod.jar 2>&1 | tee "${logfile}"
    was_killed=$(( $? == 124 ))

    t1=$(date +%s)
    duration=$(( t1 - t0 ))
    echo "Duration: ${duration} s"

    # wait for followers to stop (we need the resources, e.g. NIC, RAM, ...)
    running_nodes=""
    for node in ${nodes}; do
      if ssh "${node}" screen -ls distod-exp9-dispatchers >/dev/null; then
        running_nodes="${running_nodes}${node} "
      fi
    done
    echo "Checked node status, still running nodes: '${running_nodes}'"

    if [[ $duration -lt 30 && "${running_nodes}" != "" ]]; then
      echo "Waiting till followers stopped, before force killing them."
      sleep $(( 30 - duration ))
    fi

    while [[ "${running_nodes}" != "" ]]; do
      echo "Still waiting for nodes: '${running_nodes}'"
      for node in ${running_nodes}; do
        # still running --> kill follower
        if ssh "${node}" screen -ls distod-exp9-dispatchers >/dev/null; then
          echo "Killing follower on node ${node}"
          ssh "${node}" screen -S distod-exp9-dispatchers -X quit
        else
          # remove node from running set
          running_nodes=$( echo "${running_nodes}" | sed -s "s/${node} //" )
        fi
      done
    done

    # collect results
    echo "Collecting results for dataset ${dataset} run with dispatchers: ${master_dispatcher}/${worker_dispatcher}"
    mv distod.log "${folder}/distod-$(hostname).log"
    mv results.txt "${folder}/"
    for node in ${nodes}; do
      scp "${node}":~/distod/distod.log "${folder}/distod-${node}.log" >/dev/null
      # intentially put argument in single quotes to let the target shell expand the ~
      ssh "${node}" rm -f '~/distod/distod.log'
    done

    {
      echo -n "${dataset},${master_dispatcher},${worker_dispatcher},"
      grep "TIME Overall runtime" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
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
done

# release lock file
rm -f /var/lock/distod-exp9-dispatchers.lock
