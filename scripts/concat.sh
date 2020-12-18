#! /bin/bash

#input_dir="$1"
#output_dir="$2"

find /share/hadoop/jkarimov/workDir/StreamBenchmarks/output/spark/*/* -type f -print0 | xargs -0 -I {} cat {} >> /share/hadoop/jkarimov/workDir/StreamBenchmarks/output/results/tt.csv

