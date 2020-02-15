#!/usr/bin/env bash

# use VisualVm to connect to the process remotely via JMX: $hostname:9010
# use -verbose:gs to display GC details
java -Xms28g -Xmx28g -XX:+UseG1GC \
  -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.rmi.port=9010 -Djava.rmi.server.hostname="$(hostname)" \
  -Dconfig.file="$(hostname).conf" \
  -Dlogback.configurationFile=logback.xml \
  -jar distod.jar
