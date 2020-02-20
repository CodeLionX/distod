#!/usr/bin/env bash

datasets="letter-sub.csv"
number_nodes="1 2 3 4 5 6 7 8 9 10 11"
resultfolder="results"
resultfile="${resultfolder}/metrics.csv"

declare -a nodes=( "odin02" "odin03" "odin04" "odin05" "odin06" "odin07" "odin08" "thor01" "thor02" "thor03" "thor04" )

# write lock file
touch /var/lock/distod-exp2-nodes.lock

mkdir -p "${resultfolder}"
echo "Dataset,#Nodes,Runtime (ms)" >"${resultfile}"

for dataset in ${datasets}; do
  for n in ${number_nodes}; do
    mkdir -p "${resultfolder}/${dataset}-${n}nodes"
    logfile="${resultfolder}/${dataset}-${n}nodes/out.log"

    echo ""
    echo ""
    echo "Running DISTOD with ${n} nodes on dataset ${dataset}"

    # start followers
    for (( i=0; i<n; ++i )); do
      node="${nodes[i]}"
      ssh "${node}" "cd ~/distod && screen -d -S \"distod-exp1-datasets\" -m ./start.sh"
    done

    t0=$(date +%s)

    # start leader
    timeout --preserve-status --signal=15 24h \
      java -Xms56g -Xmx56g -XX:+UseG1GC -XX:G1ReservePercent=10 \
        -XX:MaxGCPauseMillis=400 -XX:G1HeapWastePercent=1 \
        -XX:+UnlockExperimentalVMOptions -XX:G1MixedGCLiveThresholdPercent=60 \
        -XX:G1MixedGCCountTarget=10 -XX:G1OldCSetRegionThresholdPercent=20 \
        -Dconfig.file="$(hostname).conf" \
        -Dlogback.configurationFile=logback.xml \
        -Ddistod.input.path="../data/${dataset}" \
        -Ddistod.input.has-header="no" \
        -jar distod.jar 2>&1 | tee "${logfile}"

    t1=$(date +%s)
    duration=$(( t1 - t0 ))
    echo "Duration: ${duration} s"

    # wait for followers to stop (we need the resources, e.g. NIC, RAM, ...)
    running_nodes=""
    for (( i=0; i<n; ++i )); do
      node="${nodes[i]}"
      ssh "${node}" screen -ls distod-exp1-datasets >/dev/null
      if [ $? == 0 ]; then
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
        ssh "${node}" screen -ls distod-exp1-datasets >/dev/null
        # still running --> kill follower
        if [ $? == 0 ]; then
          echo "Killing follower on node ${node}"
          ssh "${node}" screen -S distod-exp1-datasets -X quit
        else
          # remove node from running set
          running_nodes=$( echo "${running_nodes}" | sed -s "s/${node} //" )
        fi
      done
    done

    # collect results
    echo "Collecting results for dataset ${dataset}"
    mv distod.log "${resultfolder}/${dataset}/distod-odin01.log"
    mv results.txt "${resultfolder}/${dataset}/"
    for (( i=0; i<n; ++i )); do
      node="${nodes[i]}"
      scp "${node}":~/distod/distod.log "${resultfolder}/${dataset}/distod-${node}.log" >/dev/null
      # intentially put argument in single quotes to let the target shell expand the ~
      ssh "${node}" rm -f '~/distod/distod.log'
    done

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
rm -f /var/lock/distod-exp2-nodes.lock
