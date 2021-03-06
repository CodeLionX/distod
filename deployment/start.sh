#!/usr/bin/env bash

# use VisualVm to connect to the process remotely via JMX: $hostname:9010
# use -verbose:gs to display GC details
java -Xms28g -Xmx28g -XX:+UseG1GC -XX:G1ReservePercent=10 \
  -XX:MaxGCPauseMillis=400 -XX:G1HeapWastePercent=1 \
  -XX:+UnlockExperimentalVMOptions -XX:G1MixedGCLiveThresholdPercent=60 \
  -XX:G1MixedGCCountTarget=10 -XX:G1OldCSetRegionThresholdPercent=20 \
  -Dconfig.file="$(hostname).conf" \
  -Dlogback.configurationFile=logback.xml \
  -Dfile.encoding=UTF-8 \
  -jar distod.jar

#  -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
#  -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.rmi.port=9010 -Djava.rmi.server.hostname="$(hostname)" \

# use Graal JIT:
# -XX:+UnlockExperimentalVMOptions -XX:+EnableJVMCI -XX:+UseJVMCICompiler
