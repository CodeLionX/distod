#!/usr/bin/env bash

# use VisualVm to connect to the process remotely via JMX: $hostname:9010
# use -verbose:gs to display GC details
java -Xms28g -Xmx28g -XX:+UseG1GC \
  -Dconfig.file="$(hostname).conf" \
  -Dlogback.configurationFile=logback.xml \
  -jar distod.jar
