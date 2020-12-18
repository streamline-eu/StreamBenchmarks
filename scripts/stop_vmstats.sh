

#hosts='/share/hadoop/jkarimov/workDir/StreamBenchmarks/scripts/slaves'


for arg; do
      ssh $arg 'pkill -f vmstat'
done

#while IFS='' read -r line || [[ -n "$line" ]]; do
 #       ssh $line 'pkill -f vmstat' 
#done < "$hosts"
