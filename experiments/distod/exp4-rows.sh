#!/usr/bin/env bash

datasets="ncvoter-1m-19-sub.csv"
steps=10
row_limit=1000000

#datasets="adult-sub.csv"
#steps=10
#row_limit=33000

row_step=$(( row_limit / steps ))
resultfolder="results"
resultfile="${resultfolder}/metrics.csv"

declare -a nodes="odin02 odin03 odin04 odin05 odin06 odin07 odin08 thor01 thor02 thor03 thor04"

# write lock file
touch /var/lock/distod-exp4-rows.lock

mkdir -p "${resultfolder}"
echo "Dataset,#Rows,Runtime (ms)" >"${resultfile}"


for dataset in ${datasets}; do
  echo "==================================================="
  echo "Starting experiment exp4-rows on dataset ${dataset}"
  echo "Rows from: ${row_step}"
  echo "Rows until: ${row_limit}"
  echo "Stepping ${row_step} (${steps} steps)"
  echo "==================================================="

  for (( n=row_step; n<=row_limit; n=n+row_step )); do
    mkdir -p "${resultfolder}/${dataset}/${n}"
    logfile="${resultfolder}/${dataset}/${n}/out.log"

    echo ""
    echo ""
    echo "Running DISTOD on dataset ${dataset} with ${n} rows"

    # start followers
    for node in ${nodes}; do
      ssh "${node}" "cd ~/distod && screen -d -S \"distod-exp4-rows\" -m ./start.sh"
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
        -Ddistod.input.max-rows="${n}" \
        -Dfile.encoding=UTF-8 \
        -jar distod.jar 2>&1 | tee "${logfile}"
    was_killed=$(( $? != 0 ))

    t1=$(date +%s)
    duration=$(( t1 - t0 ))
    echo "Duration: ${duration} s"

    # wait for followers to stop (we need the resources, e.g. NIC, RAM, ...)
    running_nodes=""
    for node in ${nodes}; do
      if ssh "${node}" screen -ls distod-exp4-rows >/dev/null; then
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
        if ssh "${node}" screen -ls distod-exp4-rows >/dev/null; then
          echo "Killing follower on node ${node}"
          ssh "${node}" screen -S distod-exp4-rows -X quit
        else
          # remove node from running set
          running_nodes=$( echo "${running_nodes}" | sed -s "s/${node} //" )
        fi
      done
    done

    # collect results
    echo "Collecting results for dataset ${dataset} with ${n} rows"
    mv distod.log "${resultfolder}/${dataset}/${n}/distod-$(hostname).log"
    mv results.txt "${resultfolder}/${dataset}/${n}/"
    for node in ${nodes}; do
      scp "${node}":~/distod/distod.log "${resultfolder}/${dataset}/${n}/distod-${node}.log" >/dev/null
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
rm -f /var/lock/distod-exp4-rows.lock
