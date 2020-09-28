#!/usr/bin/env bash

# capture deployment path and lock file name from the ansible script as arguments:
deployment_dir=$1
lock_file=$2

#datasets="test-sub.csv iris-sub.csv chess-sub.csv abalone-sub.csv bridges-sub.csv adult-sub.csv letter-sub.csv hepatitis-sub.csv flight_1k_30c-sub.csv fd-reduced-250k-30-sub.csv flight-500k-sub.csv"
datasets="bridges-sub.csv adult-sub.csv letter-sub.csv hepatitis-sub.csv flight_1k_30c-sub.csv fd-reduced-250k-30-sub.csv flight-500k-sub.csv"
resultfolder="results"
resultfile="${resultfolder}/metrics.csv"
N=3

nodes="pi02 pi03 pi04 pi05 pi06 pi07 pi08 pi09 pi10 pi11 pi12"

# write lock file
touch "${lock_file}"

mkdir -p "${resultfolder}"
echo "Dataset,Run,Runtime (ms),#FDs,#ODs" >"${resultfile}"

for dataset in ${datasets}; do
  echo ""
  echo ""
  echo "Running DISTOD on dataset ${dataset}"

  for (( n=0; n<N; ++n )); do
    mkdir -p "${resultfolder}/${dataset}/${n}"
    logfile="${resultfolder}/${dataset}/${n}/out.log"
    echo ""
    echo "Run ${n}"

    # start followers
    for node in ${nodes}; do
      ssh "${node}" "cd ${deployment_dir} && screen -d -S \"distod-exp1-datasets\" -m ./start.sh"
    done

    t0=$(date +%s)

    # start leader
    heap_size=2700m
#    if [[ "${dataset}" == "horse-sub.csv" || "${dataset}" == "plista-sub.csv" || "${dataset}" == "ncvoter-1m-19-sub.csv" ]]; then
#      heap_size=58g
#    fi
    timeout --signal=15 24h \
      java "-Xms${heap_size}" "-Xmx${heap_size}" \
        -Dconfig.file="$(hostname).conf" \
        -Dlogback.configurationFile=logback.xml \
        -Ddistod.input.path="../data/${dataset}" \
        -Ddistod.input.has-header="no" \
        -Dfile.encoding=UTF-8 \
        -jar distod.jar 2>&1 | tee "${logfile}"
    was_killed=$(( $? == 124 ))

    t1=$(date +%s)
    duration=$(( t1 - t0 ))
    echo "Duration: ${duration} s"

    # wait for followers to stop (we need the resources, e.g. NIC, RAM, ...)
    running_nodes=""
    for node in ${nodes}; do
      if ssh "${node}" screen -ls distod-exp1-datasets >/dev/null; then
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
        if ssh "${node}" screen -ls distod-exp1-datasets >/dev/null; then
          echo "Killing follower on node ${node}"
          ssh "${node}" screen -S distod-exp1-datasets -X quit
        else
          # remove node from running set
          running_nodes=$( echo "${running_nodes}" | sed -s "s/${node} //" )
        fi
      done
    done

    # collect results
    echo "Collecting results for dataset ${dataset} run ${n}"
    mv distod.log "${resultfolder}/${dataset}/${n}/distod-$(hostname).log"
    mv results.txt "${resultfolder}/${dataset}/${n}/"
    for node in ${nodes}; do
      scp "${node}:${deployment_dir}/distod.log" "${resultfolder}/${dataset}/${n}/distod-${node}.log" >/dev/null
      # intentially put argument in single quotes to let the target shell expand the ~
      ssh "${node}" rm -f "${deployment_dir}"/distod.log
    done

    {
      echo -n "${dataset},${n},"
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

  # calculate statistics for the runtime
  runs=$( grep "${dataset}" "${resultfile}" | cut -d ',' -f3 | sort -n )
  min_runtime=$( echo "${runs}" | head -n 1 )
  max_runtime=$( echo "${runs}" | tail -n 1 )
  avg_runtime=$( echo "${runs}" | awk '{ total += $2; count++ } END { print total/count }' )
  echo "${dataset},ALL,MIN=${min_runtime}&AVG=${avg_runtime}&MAX=${max_runtime},," >>"${resultfile}"
done

# release lock file
rm -f "${lock_file}"
