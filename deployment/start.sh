#!/usr/bin/env bash

java -Xms10g -Xmx10g \
  -Dconfig.file="$(hostname).conf" \
  -Dlogback.configurationFile=logback.xml \
  -jar distod.jar
