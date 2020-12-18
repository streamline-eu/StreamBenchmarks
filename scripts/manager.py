#!/usr/biin/python
import sys
import subprocess as sp
import time
import shlex
import os
import signal
import shutil
import glob
import yaml
import socket

conf_file='/share/hadoop/jkarimov/workDir/StreamBenchmarks/conf/benchmarkConf.yaml'

with open(conf_file, 'r') as f:
    common_config = yaml.load(f)

conf_file = '/share/hadoop/jkarimov/workDir/StreamBenchmarks/conf/benchmarkConf.yaml'

def start_flink():
        sp.call([  common_config['flink_home'] + "bin/start-cluster.sh"])

def stop_flink():
        sp.call([ common_config['flink_home'] + "bin/stop-cluster.sh"])

def flink_benchmark():
        clear_dir( common_config['project_home']  + "output/flink/")
        clear_dir( common_config['project_home']  + "data-generator/stats/")
        sp.call([ common_config['flink_home'] + "bin/flink" , "run", common_config['project_home'] + "flink-benchmarks/target/flink-benchmarks-0.1.0.jar", "--confPath",conf_file])     

def start_spark():
        sp.call([  common_config['spark_home'] + "sbin/start-all.sh"])
def stop_spark():
        sp.call([ common_config['spark_home'] + "sbin/stop-all.sh" ])

def clear_dir(path):
        filelist = [ f for f in os.listdir(path) ]
        for f in filelist:
                try:
                        shutil.rmtree(path+f )
                except:
                        os.remove(path+f)

def spark_benchmark():
        clear_dir( common_config['project_home']  + "output/spark/")
        clear_dir( common_config['project_home']  + "data-generator/stats/")
        sp.call([ common_config['spark_home']  + 'bin/spark-submit' ,'--class' ,'spark.benchmark.SparkBenchmark', common_config['project_home']  + 'spark-benchmarks/target/spark-benchmarks-0.1.0.jar' , conf_file])

def start_zookeeper():
        sp.call([common_config['zk_home'] + 'bin/zkServer.sh', 'start'])        
        time.sleep(2)
def stop_zookeeper():
        sp.call([common_config['zk_home'] + 'bin/zkServer.sh', 'stop'])

def start_storm():
        storm_home = common_config['storm_home']
        zk_home = common_config['zk_home']
        start_zookeeper()
        sp.Popen([storm_home + 'bin/storm' ,'nimbus' ], stdout=sp.PIPE,stderr=sp.STDOUT)
        start_supervisor_node = '\'' +   storm_home + 'bin/storm supervisor\'' 
#       print start_supervisor_node
        time.sleep(5)
        print('nimbus started')
        sp.Popen([storm_home + 'bin/storm' ,'ui' ], stdout=sp.PIPE,stderr=sp.STDOUT)
        time.sleep(5)
        for host in common_config['slaves']:
                #print host
                sp.Popen (["ssh", host.strip() ,'\'' + start_supervisor_node + '\''   ],stdout=sp.PIPE,stderr=sp.STDOUT) 
                time.sleep(5)
                print('supervisor ' + host + ' started')
        print('storm started')
                

def stop_process(name,host):
        host  = host.strip()
        if host == socket.gethostname():
            sp.call ( ["pkill" , name] ) 
            #os.system('pkill -f ' , 'storm') 
        else:
            sp.call ([ 'ssh', host,  'pkill -f ' + name ]) 

def stop_storm():
        stop_zookeeper()
        stop_process_all('storm')

def stop_process_all(name):
        for host in common_config['nodes.all']:
                print('stopping ' + name + ' on ' + host)
                stop_process(name,host)
                time.sleep(1)

def storm_benchmark():
        clear_dir( common_config['project_home']  + "output/storm/")
        clear_dir( common_config['project_home']  + "data-generator/stats/")
        sp.call([common_config['storm_home'] + "bin/storm", "jar", common_config['project_home']  + 'storm-benchmarks/target/storm-benchmarks-0.1.0.jar' , 'storm.benchmark.StormBenchmark', conf_file,'cluster','topologyName'])

def merge_output_files():
        sp.call(["cat", common_config['spark.output']  + '*/*', '>','spark.csv'])

def concat_files_in_dir(input_dir, output_dir):
        read_files = glob.glob(input_dir )

        with open(output_dir, "wb") as outfile:
                for f in read_files:
                        if ('_temporary' in f):
                                continue
                        with open(f, "rb") as infile:
                                outfile.write(infile.read().replace('(', '').replace(')', ''))


def start_data_generators():
        clear_dir( common_config['datagenerator.stats.dir'] )
        selectivities = common_config['datagenerator.selectivities']
        for host in common_config['datasourcesocket.hosts']:
                idx = 0
                for  port in common_config['datasourcesocket.ports']:
                        start_script = '\'' +  common_config['project_home'] + 'data-generator/benchmark_data_generator ' +  str( selectivities[idx]) + ' ' + str(common_config['datagenerator.benchmarkcount']) + ' ' + str(common_config['datagenerator.loginterval']) + ' ' + str(port) + ' ' + str(common_config['datagenerator.sleep.ms']) + ' ' + str( common_config['datagenerator.nonsleepcount']) + ' ' + str(common_config['sustainability.limit']) + ' ' +  str(common_config['backpressure.limit']) + '\''
                        idx = idx+1
                        sp.Popen (["ssh", host.strip() , '\'' + start_script + '\'' ],stdout=sp.PIPE,stderr=sp.STDOUT)
                        time.sleep(1)                   
def stop_data_generators():
        stop_process_all('benchmark_data_generator')

def start_vmstats():
        nodes =  ' '.join( common_config['slaves'] ) + ' ' + common_config['master']  
        sp.call([ common_config['project_home'] + 'scripts/start_vmstats.sh ' + nodes  ],shell=True)    
def stop_vmstats():
        stop_process_all('vmstat')
#parse_conf_file()

if(len(sys.argv[1:]) == 1):
        arg = sys.argv[1]
        if (arg == "start-flink"):
                start_flink()
        elif(arg == "stop-flink"):
                stop_flink()
        elif(arg == "flink-benchmark"):
                flink_benchmark()
        elif(arg == "start-spark" ):
                start_spark()
        elif(arg == "stop-spark"):
                stop_spark()
        elif(arg == "spark-benchmark"):
                spark_benchmark()
        elif(arg == "start-zookeeper"):
                start_zookeeper()
        elif(arg == "stop-zookeeper"):
                stop_zookeeper()
        elif(arg == "start-storm"):
                start_storm()
        elif(arg == "stop-storm"):
                stop_storm()
        elif(arg == "storm-benchmark"):
                storm_benchmark()
        elif(arg == "concat-spark"):
                concat_files_in_dir('/share/hadoop/jkarimov/workDir/StreamBenchmarks/output/spark/*/*', '/share/hadoop/jkarimov/workDir/StreamBenchmarks/output/temp/spark_temp.csv')
        elif(arg == "concat-storm"):
                 concat_files_in_dir('/share/hadoop/jkarimov/workDir/StreamBenchmarks/output/storm/*','/share/hadoop/jkarimov/workDir/StreamBenchmarks/output/temp/storm_temp.csv')
        elif(arg == "concat-flink"):
                concat_files_in_dir('/share/hadoop/jkarimov/workDir/StreamBenchmarks/output/flink/*/*','/share/hadoop/jkarimov/workDir/StreamBenchmarks/output/temp/flink_temp.csv')
        elif(arg == "concat-stats"):
               concat_files_in_dir('/share/hadoop/jkarimov/workDir/StreamBenchmarks/data-generator/stats/*','/share/hadoop/jkarimov/workDir/StreamBenchmarks/output/temp/stats_temp.csv')
        elif(arg == "start-datagenerators"):
                start_data_generators()
        elif(arg == "start-vmstats"):
                start_vmstats()
        elif(arg == "stop-vmstats"):
                stop_vmstats()
        elif(arg == "stop-datagenerators"):
                stop_data_generators()
elif(len(sys.argv[1:]) == 2):
        arg1 = sys.argv[1]      
        arg2 = sys.argv[2]
        if(arg1 == "stop-process-all"):
                stop_process_all(arg2)  
        elif(arg1 == "start-datagenerator"):
                start_data_generator(arg2)
