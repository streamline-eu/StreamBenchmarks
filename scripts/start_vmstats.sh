#!/bin/bash

#hosts='/share/hadoop/jkarimov/workDir/StreamBenchmarks/scripts/slaves'

rm -rf /share/hadoop/jkarimov/workDir/StreamBenchmarks/vmstats/*


for arg; do
   ssh $arg 'bash -s' < /share/hadoop/jkarimov/workDir/StreamBenchmarks/scripts/start_vmstat.sh
done

#while IFS='' read -r line || [[ -n "$line" ]]; do
#	ssh $line 'bash -s' < start_vmstat.sh
#done < "$hosts"


#ssh cloud-12 'bash -s' < start_vmstat.sh
