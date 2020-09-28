#!/usr/bin/env bash

dataset="letter-sub.csv"
resultfolder="results"
resultfile="${resultfolder}/metrics.csv"

N=5

# Allow usage of sdkman!
export SDKMAN_DIR="$HOME/.sdkman"
# shellcheck source=/dev/null
source "$HOME/.sdkman/bin/sdkman-init.sh"

# test matrix
# JVM JIT GC Version
# ===================================
# Zing Falcon C4 8.0.265
# Zing Falcon C4 11.0.8
# Zulu HotSpot G1 11.0.8
# OracleJDK HotSpot G1 11.0.8
# GraalVM Graal G1 11.0.8
# AdoptOpenJDK HotSpot G1 8.0.265
# AdoptOpenJDK HotSpot G1 15.0.0
# AdoptOpenJDK HotSpot G1 11.0.8
# AdoptOpenJDK HotSpot ParallelGC 11.0.8
# AdoptOpenJDK HotSpot CMS 11.0.8
# AdoptOpenJDK HotSpot ZGC 11.0.8
# AdoptOpenJDK HotSpot Shenandoah 15.0.0
# AdoptOpenJDK OpenJ9 gencon 11.0.8
# AdoptOpenJDK OpenJ9 balanced 11.0.8
# AdoptOpenJDK OpenJ9 metronome 11.0.8
# AdoptOpenJDK OpenJ9 optavgpause 11.0.8
# OracleJDK HotSpot G1 8.0.261
# OracleJDK HotSpot G1 15.0.0

declare -a jmvs=(
  "8.0.265-zing"
  "11.0.8-zing"
  "11.0.8-zulu"
  "11.0.8-oracle"
  "20.2.0.r11-grl"
  "8.0.265.hs-adpt"
  "15.0.0.hs-adpt"
  "11.0.8.hs-adpt"
  "11.0.8.hs-adpt"
  "11.0.8.hs-adpt"
  "11.0.8.hs-adpt"
  "15.0.0.hs-adpt"
  "11.0.8.j9-adpt"
  "11.0.8.j9-adpt"
  # "11.0.8.j9-adpt" # does only support compressed Oops and <= 4GB heap
  "11.0.8.j9-adpt"
  "8.0.261-oracle"
  "15.0.0-oracle"
)
declare -a argss=(
  "" # zing needs no args
  "" # zing needs no args
  "-XX:+UseG1GC"
  "-XX:+UseG1GC"
  "-XX:+UseG1GC"
  "-XX:+UseG1GC"
  "-XX:+UseG1GC"
  "-XX:+UseG1GC"
  "-XX:+UseParallelGC"
  "-XX:+UseConcMarkSweepGC"
  "-XX:+UnlockExperimentalVMOptions -XX:+UseZGC"
  "-XX:+UseShenandoahGC"
  "-Xgcpolicy:gencon -Dakka.java-flight-recorder.enabled=false"
  "-Xgcpolicy:balanced -Dakka.java-flight-recorder.enabled=false"
  # "-Xgcpolicy:metronome -Dakka.java-flight-recorder.enabled=false"
  "-Xgcpolicy:optavgpause -Dakka.java-flight-recorder.enabled=false"
  "-XX:+UseG1GC"
  "-XX:+UseG1GC"
)
declare -a gcnames=(
  "C4"
  "C4"
  "G1"
  "G1"
  "G1"
  "G1"
  "G1"
  "G1"
  "Parallel"
  "CMS"
  "Z"
  "Shenandoah"
  "gencon"
  "balanced"
  # "metronome"
  "optavgpause"
  "G1"
  "G1"
)

nodes="thor01 thor02 thor03 thor04 odin02 odin03 odin04 odin05 odin06 odin07 odin08"

# write lock file
touch /var/lock/distod-exp8-jvms.lock

mkdir -p "${resultfolder}"
echo "JVM,GC,Run,Overall Runtime (s),Measured Runtime (ms),#FDs,#ODs" >"${resultfile}"

for ((i = 0; i < ${#jmvs[@]}; ++i)); do
  jvm="${jmvs[i]}"
  args="${argss[i]}"
  gcname="${gcnames[i]}"

  echo ""
  echo ""
  echo "Running DISTOD on dataset ${dataset}"
  echo "  JVM: ${jvm}"
  echo "  JVM args: ${args}"

  echo "Switching JVM to ${jvm} using sdkman!"
  switch_java_command="sdk use java ${jvm}"
  if ! ${switch_java_command} >/dev/null; then
    echo "${node} could not switch to JVM ${jvm}!!"
    exit 1
  fi

  for ((n = 0; n < N; ++n)); do
    folder="${resultfolder}/${jvm}-${gcname}/${n}"
    mkdir -p "${folder}"
    logfile="${folder}/out.log"
    echo ""
    echo "Run ${n}"

    # system metrics file: sys_metrics.log
    # headers: percent CPU usage, percent memory usage, virtual memory in KiB, cumulative CPU time
    run_command="export SDKMAN_DIR=\"\$HOME/.sdkman\"; \
      source \"\$HOME/.sdkman/bin/sdkman-init.sh\"; \
      ${switch_java_command}; \
      java -version; \
      (while sleep 5; do ps -o pcpu,pmem,vsz,time,args -e | grep \"java.*distod.jar\$\" | sort -r | head -n 1 >> sys-metrics.log; done) & \
      metrics_pid=\$!; \
      java -server -Xmx28g ${args} \
        -Dconfig.file=\$(hostname).conf \
        -Dlogback.configurationFile=logback.xml \
        -Dfile.encoding=UTF-8 \
        -jar distod.jar; \
      kill -15 \${metrics_pid}"

    # start followers
    for node in ${nodes}; do
      ssh "${node}" "cd ~/distod && screen -d -L -S \"distod-exp8-jvms\" -m bash -c '${run_command}'"
    done

    java -version 2>&1 | tee -a "${logfile}"
    # start system metrics collector in background
    ( while sleep 5; do
      ps -o pcpu,pmem,vsz,time,args -e | grep 'java.*distod.jar$' | sort -r | head -n 1 >> sys-metrics.log
    done ) &
    metrics_pid=$!
    # start leader
    t0=$(date +%s)
    timeout --signal=15 12h \
      java -server -Xmx31g ${args} \
      -Dconfig.file="$(hostname).conf" \
      -Dlogback.configurationFile=logback.xml \
      -Ddistod.input.path="../data/${dataset}" \
      -Ddistod.input.has-header="no" \
      -Dfile.encoding=UTF-8 \
      -jar distod.jar 2>&1 | tee -a "${logfile}"
    t1=$(date +%s)

    # wait for java process to finish and then stop system metrics collection
    kill -15 ${metrics_pid}

    duration=$((t1 - t0))
    echo "Duration: ${duration} s"

    # wait for followers to stop (we need the resources, e.g. NIC, RAM, ...)
    running_nodes=""
    for node in ${nodes}; do
      if ssh "${node}" screen -ls distod-exp8-jvms >/dev/null; then
        running_nodes="${running_nodes}${node} "
      fi
    done
    echo "Checked node status, still running nodes: '${running_nodes}'"

    if [[ $duration -lt 30 && "${running_nodes}" != "" ]]; then
      echo "Waiting till followers stopped, before force killing them."
      sleep $((30 - duration))
    fi

    while [[ "${running_nodes}" != "" ]]; do
      echo "Still waiting for nodes: '${running_nodes}'"
      for node in ${running_nodes}; do
        # still running --> kill follower
        if ssh "${node}" screen -ls distod-exp8-jvms >/dev/null; then
          echo "Killing follower on node ${node}"
          ssh "${node}" screen -S distod-exp8-jvms -X quit
        else
          # remove node from running set
          running_nodes=$(echo "${running_nodes}" | sed -s "s/${node} //")
        fi
      done
    done

    # collect results
    echo "Collecting results for JVM ${jvm} with ${gcname}GC, run ${n}"
    mv distod.log "${folder}/distod-$(hostname).log"
    mv sys-metrics.log "${folder}/sys-metrics-$(hostname).log"
    mv results.txt "${folder}/"
    for node in ${nodes}; do
      scp "${node}":~/distod/distod.log "${folder}/distod-${node}.log" >/dev/null
      scp "${node}":~/distod/sys-metrics.log "${folder}/sys-metrics-${node}.log" >/dev/null
      scp "${node}":~/distod/screenlog.0 "${folder}/screen-${node}.log" >/dev/null
      # intentially put argument in single quotes to let the target shell expand the ~
      ssh "${node}" rm -f '~/distod/distod.log' '~/distod/sys-metrics.log' '~/distod/screenlog.0'
    done

    {
      echo -n "${jvm},${gcname},${n},${duration},"
      grep "TIME Overall runtime" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/\([[:space:]]\|[a-zA-Z]\)*$//' | tr -d '\n'
      echo -n ","
      grep "# FD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'
      echo -n ","
      grep "# OD" "${logfile}" | tail -n 1 | cut -d ':' -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\n'
      # force newline
      echo ""
    } >>"${resultfile}"

  done
done

# release lock file
rm -f /var/lock/distod-exp8-jvms.lock
