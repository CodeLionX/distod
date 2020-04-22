#!/usr/bin/env bash

jar=distod.jar
config=bench.conf

filename="bench-results-workers.txt"
echo "Benchmarking DISTOD" >"${filename}"
{
  printf "==================================\nConfig:\n"
  cat "${config}"
  printf "\n\n"
} >>"${filename}"

for i in {1..20}; do
    echo "Test ${i}:"
    result=$(
      java -Xmx10g -Xms10g \
        -Dconfig.file="${config}" \
        -Dlogback.configurationFile=logback.xml \
        -Ddistod.max-workers="${i}" \
        -Dfile.encoding=UTF-8 \
        -jar "${jar}"
    )

    printf "Test %d workers:\n%s\n" "${i}" "${result}" >>"${filename}"
done
