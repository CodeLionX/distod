#!/usr/bin/env bash

hosts="odin01 odin02 odin03 odin04 odin05 odin06 odin07 odin08 thor01 thor02 thor03 thor04"

for host in ${hosts}; do
  echo "Running '$*' with host = ${host}"
  eval "$@" &
done

wait
