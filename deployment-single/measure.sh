#!/usr/bin/env bash

jar=distod.jar
config=bench.conf

files=( "flight_1k_5c" "flight_1k_10c" "flight_1k_15c" "flight_1k_20c" "flight_1k_25c" "flight_1k_30c" )
#header=( "no" "no" ) #"no" "yes" "yes" )

for (( i=0; i<${#files[@]}; ++i )); do
  file=${files[i]}
  #hasHeader=${header[i]}

  filename="bench-timing-${file}.txt"
  echo "Benchmarking dataset ${file}"
  echo "Benchmarking dataset ${file}" >"${filename}"
  {
    printf "==================================\nConfig:\n"
    cat "${config}"
    printf "\n\n"
  } >>"${filename}"

  for j in {0..5}; do
      echo "Test ${j}:"
      result=$(
        taskset --cpu-list 1 java -Xmx8g -Xms8g \
          -Dconfig.file="${config}" \
          -Dlogback.configurationFile=logback.xml \
          -Ddistod.input.path="data/${file}.csv" \
          -Ddistod.input.has-header="yes" \
          -jar "${jar}" | grep "TIME"
      )

      printf "Test %d:\n%s\n" "${j}" "${result}" >>"${filename}"
  done
done
