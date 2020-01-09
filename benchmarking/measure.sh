#!/usr/bin/env bash

jar=distod-trie.jar
config=bench.conf
workers=1

filename="bench-results-iris-${jar}-${workers}w.txt"
echo "Benchmarking ${jar}" >"${filename}"
{
  printf "==================================\nConfig:\n"
  cat "${config}"
  printf "\n\n"
} >>"${filename}"

for i in {0..10}; do
    echo "Test ${i}:"
    result=$(
      java -Xmx8g -Xms8g \
        -Dconfig.file="${config}" \
        -Dlogback.configurationFile=logback.xml \
        -Ddistod.max-workers="${workers}" \
        -jar "${jar}"
    )

    printf "Test %d:\n%s\n" "${i}" "${result}" >>"${filename}"
done
