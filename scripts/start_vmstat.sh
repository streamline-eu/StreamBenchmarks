#!/bin/bash
path='/share/hadoop/jkarimov/workDir/StreamBenchmarks/vmstats/'
function eg {
  while read line
  do
    printf "$line"
    date '+ %m-%d-%Y %H:%M:%S'
  done
}

vmstat 1 3600 | eg > $path$HOSTNAME.txt  &
