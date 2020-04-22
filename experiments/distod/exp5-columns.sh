#!/usr/bin/env bash

datasets="plista-rand-cols-sub.csv"
column_limit=63

col_step=5
resultfolder="results"
resultfile="${resultfolder}/metrics.csv"

declare -a nodes="odin02 odin03 odin04 odin05 odin06 odin07 odin08 thor01 thor02 thor03 thor04"

# write lock file
touch /var/lock/distod-exp5-columns.lock

mkdir -p "${resultfolder}"
echo "Dataset,#Columns,Runtime (ms)" >"${resultfile}"


for dataset in ${datasets}; do
  echo "==================================================="
  echo "Starting experiment exp5-columns on dataset ${dataset}"
  echo "Columns from: 5"
  echo "Columns until: ${column_limit}"
  echo "Stepping ${col_step} ($(( column_limit / col_step )) steps)"
  echo "==================================================="

  for (( n=5; n<=column_limit; n=n+col_step )); do
    mkdir -p "${resultfolder}/${dataset}/${n}"
    logfile="${resultfolder}/${dataset}/${n}/out.log"

    echo ""
    echo ""
    echo "Running DISTOD on dataset ${dataset} with ${n} columns"

    # start followers
    for node in ${nodes}; do
      ssh "${node}" "cd ~/distod && screen -d -S \"distod-exp5-columns\" -m ./start.sh"
    done

    t0=$(date +%s)

    # start leader
    timeout --signal=15 24h \
      java -Xms31g -Xmx31g -XX:+UseG1GC -XX:G1ReservePercent=10 \
        -XX:MaxGCPauseMillis=400 -XX:G1HeapWastePercent=1 \
        -XX:+UnlockExperimentalVMOptions -XX:G1MixedGCLiveThresholdPercent=60 \
        -XX:G1MixedGCCountTarget=10 -XX:G1OldCSetRegionThresholdPercent=20 \
        -Dconfig.file="$(hostname).conf" \
        -Dlogback.configurationFile=logback.xml \
        -Ddistod.input.path="../data/${dataset}" \
        -Ddistod.input.has-header="no" \
        -Ddistod.input.max-columns="${n}" \
        -Dfile.encoding=UTF-8 \
        -jar distod.jar 2>&1 | tee "${logfile}"
    was_killed=$(( $? != 0 ))

    t1=$(date +%s)
    duration=$(( t1 - t0 ))
    echo "Duration: ${duration} s"

    # wait for followers to stop (we need the resources, e.g. NIC, RAM, ...)
    running_nodes=""
    for node in ${nodes}; do
      if ssh "${node}" screen -ls distod-exp5-columns >/dev/null; then
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
        if ssh "${node}" screen -ls distod-exp5-columns >/dev/null; then
          echo "Killing follower on node ${node}"
          ssh "${node}" screen -S distod-exp5-columns -X quit
        else
          # remove node from running set
          running_nodes=$( echo "${running_nodes}" | sed -s "s/${node} //" )
        fi
      done
    done

    # collect results
    echo "Collecting results for dataset ${dataset} with ${n} columns"
    mv distod.log "${resultfolder}/${dataset}/${n}/distod-$(hostname).log"
    mv results.txt "${resultfolder}/${dataset}/${n}/"
    for node in ${nodes}; do
      scp "${node}":~/distod/distod.log "${resultfolder}/${dataset}/${n}/distod-${node}.log" >/dev/null
      # intentionally put argument in single quotes to let the target shell expand the ~
      ssh "${node}" rm -f '~/distod/distod.log'
    done

    {
      echo -n "${dataset},"
      echo -n "${n},"
      grep "TIME Overall runtime" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
      # force newline
      echo ""
    } >>"${resultfile}"

    # do not perform other runs if it hit the timelimit
    if (( was_killed )); then
      echo "Run exceeded timelimit, experiment finished early. Continuing with next dataset ..."
      break
    else
      echo "Was not killed by TL, continuing ..."
    fi
  done
done

# release lock file
rm -f /var/lock/distod-exp5-columns.lock
