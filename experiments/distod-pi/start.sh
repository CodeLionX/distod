#!/usr/bin/env bash

# use VisualVm to connect to the process remotely via JMX: $hostname:9010
# use -verbose:gs to display GC details
java -Xms2700m -Xmx2700m \
  -Dconfig.file="$(hostname).conf" \
  -Dlogback.configurationFile=logback.xml \
  -Dfile.encoding=UTF-8 \
  "$@" \
  -jar distod.jar
