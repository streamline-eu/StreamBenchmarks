/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.CommonConfig;
import data.source.model.AdsEvent;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-FlinkBenchmark.jar  --confPath "../conf/benchmarkConf.yaml"
 */
public class FlinkBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkBenchmark.class);


    public static void main(final String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new Exception("configuration file parameter is needed. Ex: --confPath ../conf/benchmarkConf.yaml");
        }
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String confFilePath = parameterTool.getRequired("confPath");
        CommonConfig.initializeConfig(confFilePath);

        //TODO parametertool, checkpoint flush rate, kafka zookeeper configurations

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(CommonConfig.FLUSH_RATE());
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        if (CommonConfig.BENCHMARKING_USECASE().equals(CommonConfig.AGGREGATION_USECASE)) {
            keyedWindowedAggregationBenchmark(env);
        } else if (CommonConfig.BENCHMARKING_USECASE().equals(CommonConfig.JOIN_USECASE)){
            windowedJoin(env);
        } else if (CommonConfig.BENCHMARKING_USECASE().equals(CommonConfig.ALLWINDOWED_AGGREGATION_USECASE)){
            allWindowedAggregationBenchmark(env);
        } else if(CommonConfig.BENCHMARKING_USECASE().equals(CommonConfig.DUMMY_CONSUMER)){
            dummyConsumer(env);
        }

        else {
            throw new Exception("Please specify use-case name");
        }

        env.execute();

    }


    private static void dummyConsumer(StreamExecutionEnvironment env){
        DataStream<String> socketSource = null;
        for (String host : CommonConfig.DATASOURCE_HOSTS()) {
            for (int port: CommonConfig.DATASOURCE_PORTS()){
                DataStream<String> socketSource_i = env.socketTextStream(host, port);
                socketSource = socketSource == null ? socketSource_i : socketSource.union(socketSource_i);
            }
        }
        DataStream<String> filteredStream = socketSource.filter(t->false);
        RollingSink sink = new RollingSink<String>(CommonConfig.FLINK_OUTPUT());
        sink.setBatchSize(1024 * CommonConfig.OUTPUT_BATCHSIZE_KB()); // this is 400 MB,

        filteredStream.addSink(sink);


    }




    private static void windowedJoin(StreamExecutionEnvironment env){
        class Deserializer implements MapFunction<String, Tuple4<String, Long, Double, Long>>{
            @Override
            public Tuple4<String, Long, Double, Long> map(String s) throws Exception {
                JSONObject obj = new JSONObject(s);
                String geo = obj.getString("key");
                Double price = obj.getDouble("value");
                Long ts =  obj.getLong("ts");
                return new Tuple4<String, Long, Double, Long>(geo, ts , price, System.currentTimeMillis());
            }

        }
        DataStream<String> joinStream1 = null;
        DataStream<String> joinStream2 = null;

        for (String host: CommonConfig.DATASOURCE_HOSTS()) {
            for (int i = 0; i < CommonConfig.DATASOURCE_PORTS().size(); i++){
                int port = CommonConfig.DATASOURCE_PORTS().get(i);
                DataStream<String> socketSource_i = env.socketTextStream(host, port);
                if (i % 2 == 0) {
                    joinStream1 = joinStream1 == null ? socketSource_i : joinStream1.union(socketSource_i);
                } else {
                    joinStream2 = joinStream2 == null ? socketSource_i : joinStream2.union(socketSource_i);
                }
            }
        }

        DataStream<Tuple4<String, Long, Double, Long>> projectedStream1 = joinStream1.map(new Deserializer());
        DataStream<Tuple4<String, Long, Double, Long>> projectedStream2 = joinStream2.map(new Deserializer());

        DataStream< Tuple2<Long, Long>> joinedStream = projectedStream1.join(projectedStream2).
                where(new KeySelector<Tuple4<String, Long, Double, Long>, String>() {

                    @Override
                    public String getKey(Tuple4<String, Long, Double, Long> tuple) throws Exception {
                        return tuple.f0;
                    }
                }).
                equalTo(new KeySelector<Tuple4<String, Long, Double, Long>, String>() {
                    @Override
                    public String getKey(Tuple4<String, Long, Double, Long> tuple) throws Exception {
                        return tuple.f0;
                    }
                }).
                window(SlidingProcessingTimeWindows.of(Time.milliseconds(CommonConfig.SLIDING_WINDOW_LENGTH()), Time.milliseconds(CommonConfig.SLIDING_WINDOW_SLIDE())))
                .apply(new JoinFunction<Tuple4<String, Long, Double, Long>, Tuple4<String, Long, Double, Long>, Tuple2<Long, Long>>() {

                    @Override
                    public Tuple2<Long, Long> join(Tuple4<String, Long, Double, Long> t1, Tuple4<String, Long, Double, Long> t2) throws Exception {
                        Long latency = Math.max(t1.f1,t2.f1);
                        Long startTS =  latency == t1.f1 ? t1.f3 : t2.f3;
                        return new Tuple2<>(latency, startTS);
                    }
                });


        DataStream<Tuple3<Long, Long, Long>> resultingStream = joinedStream.filter(x -> x.f1 % CommonConfig.JOIN_FILTER_FACTOR() == 0)
                .map(new MapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>>() {
            @Override
            public Tuple3<Long, Long, Long> map(Tuple2<Long, Long> l) throws Exception {
                return new Tuple3< Long, Long, Long>( System.currentTimeMillis()  - l.f0, l.f0, l.f1);
            }
        });



        RollingSink sink = new RollingSink<String>(CommonConfig.FLINK_OUTPUT());
        sink.setBatchSize(1024 * CommonConfig.OUTPUT_BATCHSIZE_KB()); // this is 400 MB,

        resultingStream.addSink(sink);

    }




    private static void keyedWindowedAggregationBenchmark(StreamExecutionEnvironment env){
        DataStream<String> socketSource = null;
        for (String host : CommonConfig.DATASOURCE_HOSTS()) {
            for (Integer port: CommonConfig.DATASOURCE_PORTS()){
                DataStream<String> socketSource_i = env.socketTextStream(host, port);
                socketSource = socketSource == null ? socketSource_i : socketSource.union(socketSource_i);
            }
        }

        DataStream<Tuple6<String, Long, Double, Integer,Integer, Long>> messageStream = socketSource.map(new MapFunction<String, Tuple6<String, Long, Double, Integer,Integer, Long>>() {
                    @Override
                    public Tuple6<String, Long, Double, Integer,Integer, Long> map(String s) throws Exception {
                        JSONObject obj = new JSONObject(s);
                        String geo = obj.getString("key");
                        Double price = obj.getDouble("value");
                        Long ts =  obj.getLong("ts");
                        return new Tuple6<String, Long, Double, Integer,Integer, Long>(geo, ts , price, 1 ,1, System.currentTimeMillis());
                    }
                });

        DataStream<Tuple6<String, Long, Double, Integer,Integer, Long>> aggregatedStream = messageStream.keyBy(0)
                .timeWindow(Time.milliseconds(CommonConfig.SLIDING_WINDOW_LENGTH()), Time.milliseconds(CommonConfig.SLIDING_WINDOW_SLIDE())).
                        reduce(new ReduceFunction<Tuple6<String, Long, Double, Integer,Integer, Long>>() {
                            @Override
                            public Tuple6<String, Long, Double, Integer,Integer, Long> reduce(Tuple6<String, Long, Double, Integer,Integer, Long> t1, Tuple6<String, Long, Double, Integer,Integer, Long> t2) throws Exception {
                                Double avgPrice = (t1.f3 * t1.f2 + t2.f3 * t2.f2) / t1.f3 + t2.f3;
                                Integer avgCount = t1.f3 + t2.f3;
                                Integer windowCount = t1.f4 + t2.f4;
                                Long ts = Math.max(t1.f1, t2.f1);
                                Long startTS = ts == t1.f1 ? t1.f5 : t2.f5;
                                return new Tuple6<String, Long, Double, Integer,Integer, Long>(t1.f0, ts, avgPrice, avgCount,windowCount, startTS);
                            }
                        });


        DataStream<Tuple6<String, Long, Double,Integer,Long, Long>> mappedStream = aggregatedStream.map(new MapFunction<Tuple6<String, Long, Double, Integer, Integer, Long>, Tuple6<String, Long, Double, Integer, Long, Long>>() {
            @Override
            public Tuple6<String, Long, Double,Integer,Long, Long> map(Tuple6<String, Long, Double, Integer, Integer, Long> t1) throws Exception {
                return new Tuple6<String, Long, Double,Integer,Long, Long>(t1.f0, System.currentTimeMillis()  - t1.f1, t1.f2,t1.f4, t1.f1, t1.f5);
            }
        });


        RollingSink sink = new RollingSink<String>(CommonConfig.FLINK_OUTPUT());
        sink.setBatchSize(1024 * CommonConfig.OUTPUT_BATCHSIZE_KB()); // this is 400 MB,

        mappedStream.addSink(sink);
    }






    private static void allWindowedAggregationBenchmark(StreamExecutionEnvironment env){
        DataStream<String> socketSource = null;
        for (String host : CommonConfig.DATASOURCE_HOSTS()) {
            for (Integer port: CommonConfig.DATASOURCE_PORTS()){
                DataStream<String> socketSource_i = env.socketTextStream(host, port);
                socketSource = socketSource == null ? socketSource_i : socketSource.union(socketSource_i);
            }
        }

        DataStream<Tuple6<String, Long, Double, Integer,Integer, Long>> messageStream = socketSource.map(new MapFunction<String, Tuple6<String, Long, Double, Integer,Integer, Long>>() {
            @Override
            public Tuple6<String, Long, Double, Integer,Integer, Long> map(String s) throws Exception {
                JSONObject obj = new JSONObject(s);
                String geo = obj.getString("key");
                Double price = obj.getDouble("value");
                Long ts =  obj.getLong("ts");
                return new Tuple6<String, Long, Double, Integer,Integer, Long>(geo, ts , price, 1 ,1, System.currentTimeMillis());
            }
        });

        DataStream<Tuple6<String, Long, Double, Integer,Integer, Long>> aggregatedStream = messageStream
                .timeWindowAll(Time.milliseconds(CommonConfig.SLIDING_WINDOW_LENGTH()), Time.milliseconds(CommonConfig.SLIDING_WINDOW_SLIDE())).
                        reduce(new ReduceFunction<Tuple6<String, Long, Double, Integer,Integer, Long>>() {
                            @Override
                            public Tuple6<String, Long, Double, Integer,Integer, Long> reduce(Tuple6<String, Long, Double, Integer,Integer, Long> t1, Tuple6<String, Long, Double, Integer,Integer, Long> t2) throws Exception {
                                Double avgPrice = (t1.f3 * t1.f2 + t2.f3 * t2.f2) / t1.f3 + t2.f3;
                                Integer avgCount = t1.f3 + t2.f3;
                                Integer windowCount = t1.f4 + t2.f4;
                                Long ts = Math.max(t1.f1, t2.f1);
                                Long startTS = ts == t1.f1 ? t1.f5 : t2.f5;
                                return new Tuple6<String, Long, Double, Integer,Integer, Long>(t1.f0, ts, avgPrice, avgCount,windowCount, startTS);
                            }
                        });


        DataStream<Tuple6<String, Long, Double,Integer,Long, Long>> mappedStream = aggregatedStream.map(new MapFunction<Tuple6<String, Long, Double, Integer, Integer, Long>, Tuple6<String, Long, Double, Integer, Long, Long>>() {
            @Override
            public Tuple6<String, Long, Double,Integer,Long, Long> map(Tuple6<String, Long, Double, Integer, Integer, Long> t1) throws Exception {
                return new Tuple6<String, Long, Double,Integer,Long, Long>(t1.f0, System.currentTimeMillis()  - t1.f1, t1.f2,t1.f4, t1.f1, t1.f5);
            }
        });


        RollingSink sink = new RollingSink<String>(CommonConfig.FLINK_OUTPUT());
        sink.setBatchSize(1024 * CommonConfig.OUTPUT_BATCHSIZE_KB()); // this is 400 MB,

        mappedStream.addSink(sink);
    }


}


