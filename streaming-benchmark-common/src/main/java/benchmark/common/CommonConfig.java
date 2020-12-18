package benchmark.common;

import com.esotericsoftware.yamlbeans.YamlReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by jeka01 on 03/11/2016.
 */
public class CommonConfig {
    private static final Logger LOG = LoggerFactory.getLogger(CommonConfig.class);

    private static String SPARK_BATCHTIME = "spark.batchtime";
    private static String SPARK_MASTER = "spark.master";
    private static String DATASOURCE_PORTS = "datasourcesocket.ports";
    private static String DATASOURCE_HOSTS = "datasourcesocket.hosts";

    private static String FLUSH_RATE = "flush.rate";
    private static String FLINK_OUTPUT = "flink.output";
    private static String SPARK_OUTPUT = "spark.output";
    private static String STORM_OUTPUT = "storm.output";
    private static String HDFS_URI = "output.hdfs.url";


    private static String SLIDING_WINDOW_LENGTH = "slidingwindow.length";
    private static String SLIDING_WINDOW_SLIDE = "slidingwindow.slide";

    private static String BENCHMARKING_USECASE = "benchmarking.usecase";
    private static String NODE_CORES = "node.cores";
    private static String OUTPUT_BATCHSIZE_KB = "output.batchsize.kb";
    private static String STORM_WORKERS = "storm.workers";
    private static String STORM_ACKERS = "storm.ackers";
    private static String PARALLELISM = "parallelism";
    private static String OUTPUT_SYNC_POLICY_COUNT = "output.sync.policy.count";

    public static String AGGREGATION_USECASE = "KeyedWindowedAggregation";
    public static String JOIN_USECASE = "WindowedJoin";
    public static String ALLWINDOWED_AGGREGATION_USECASE = "GlobalWindowedAggregation";
    public static String DUMMY_CONSUMER = "DummyConsumer";
    public static String SPARK_WINDOW_USE = "spark.window.use";
    public static String JOIN_FILTER_FACTOR = "join.filter.factor";

    private static HashMap instance = null;

    public static void initializeConfig(String confPath) {
        try {
            YamlReader reader = new YamlReader(new FileReader(confPath));
            Object object = reader.read();
            instance = (HashMap) object;
        } catch (Exception e) {
            LOG.error("Error in creating config object");
        }
    }
    public static int STORM_WORKERS() {
        return new Integer(instance.get(STORM_WORKERS).toString());
    }
    public static int STORM_ACKERS() {
        return new Integer(instance.get(STORM_ACKERS).toString());
    }
    public static int PARALLELISM() {
        return new Integer(instance.get(PARALLELISM).toString());
    }

    public static int SPARK_BATCHTIME() {
        return new Integer(instance.get(SPARK_BATCHTIME).toString());
    }
    public static String SPARK_MASTER() {
        return instance.get(SPARK_MASTER).toString();
    }
    public static List<Integer> DATASOURCE_PORTS() {
        List<Integer> myList = new ArrayList<>();
        for(String port: (List<String> )instance.get(DATASOURCE_PORTS)){
            myList.add(new Integer(port));
        }
        return myList;
    }
    public static List<String> DATASOURCE_HOSTS() {
        return (List<String>) instance.get(DATASOURCE_HOSTS);
    }
    public static long FLUSH_RATE() {
        return new Long(instance.get(FLUSH_RATE).toString());
    }
    public static String FLINK_OUTPUT() {
        return instance.get(FLINK_OUTPUT).toString();
    }
    public static String SPARK_OUTPUT() {
        return instance.get(SPARK_OUTPUT).toString();
    }
    public static String STORM_OUTPUT() {
        return instance.get(STORM_OUTPUT).toString();
    }
    public static int SLIDING_WINDOW_LENGTH() {return new Integer(instance.get(SLIDING_WINDOW_LENGTH).toString());}
    public static int SLIDING_WINDOW_SLIDE() {
        return new Integer(instance.get(SLIDING_WINDOW_SLIDE).toString());
    }
    public static String BENCHMARKING_USECASE() {
        return instance.get(BENCHMARKING_USECASE).toString();
    }
    public static int NODE_CORES() {
        return new Integer(instance.get(NODE_CORES).toString());
    }
    public static long OUTPUT_BATCHSIZE_KB() {
        return new Long(instance.get(OUTPUT_BATCHSIZE_KB).toString());
    }
    public static String HDFS_URI() {
        return instance.get(HDFS_URI).toString();
    }
    public static int OUTPUT_SYNC_POLICY_COUNT() {return new Integer(instance.get(OUTPUT_SYNC_POLICY_COUNT).toString());}
    public static boolean SPARK_WINDOW_USE() {return new Boolean(instance.get(SPARK_WINDOW_USE).toString());}
    public static int JOIN_FILTER_FACTOR() {return new Integer(instance.get(JOIN_FILTER_FACTOR).toString());}

}
