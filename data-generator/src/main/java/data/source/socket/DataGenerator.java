package data.source.socket;

import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.model.AdsEvent;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Created by jeka01 on 02/09/16.
 */
public class DataGenerator extends Thread {
    private int benchmarkCount;
    private long sleepTime;
    private static Double partition;
    private BlockingQueue<String> buffer;
    private AdsEvent adsEvent;
    private HashMap<Long, Integer> bufferSizeAtTime = new HashMap<>();

    private HashMap<Long,Integer> dataGenRate = new HashMap<>();

    private DataGenerator(HashMap conf, BlockingQueue<String> buffer) throws IOException {
        this.buffer = buffer;
        this.benchmarkCount = new Integer(conf.get("benchmarking.count").toString());
        this.sleepTime = new Long(conf.get("datagenerator.sleep").toString());
        adsEvent = new AdsEvent( partition);
    }

    public void run() {
        try {
            sendTuples(benchmarkCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void sendTuples(int tupleCount) throws Exception {
        long currTime = System.currentTimeMillis();
        int tempVal = 0;
        if (sleepTime != 0) {
            for (int i = 0; i < tupleCount; ) {
                Thread.sleep(sleepTime);
                for (int b = 0; b < 1 && i < tupleCount; b++, i++) {
                    buffer.put(adsEvent.generateJson());
                    if (i % 1000 == 0){
                        long interval = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
                        int bufferSize = buffer.size();
                        bufferSizeAtTime.put(interval, bufferSize);
                        dataGenRate.put(interval, i - tempVal);
                        tempVal = i;
                    }
                }
            }
        } else {
            for (int i = 0; i < tupleCount; ) {
                for (int b = 0; b < 1 && i < tupleCount; b++, i++) {
                    buffer.put(adsEvent.generateJson());
                    if (i % 1000 == 0){
                        long interval = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
                        int bufferSize = buffer.size();
                        bufferSizeAtTime.put(interval, bufferSize);
                        dataGenRate.put(interval, i - tempVal);
                        tempVal = i;
                    }
                }
            }
        }
        long runtime = (currTime - System.currentTimeMillis()) / 1000;
        System.out.println("Benchmark producer data rate is " + tupleCount / runtime + " ps");
    }

    public static void main(String[] args) throws Exception {
        String confFilePath = args[0];
        partition = new Double(args[1]);
        YamlReader reader = new YamlReader(new FileReader(confFilePath));
        Object object = reader.read();
        HashMap conf = (HashMap) object;

        Integer port = new Integer(conf.get("datasourcesocket.port").toString());
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(900000);
        System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
        Socket server = serverSocket.accept();
        System.out.println("Just connected to " + server.getRemoteSocketAddress());
        PrintWriter out = new PrintWriter(server.getOutputStream(), true);
        int bufferSize = new Integer(conf.get("benchmarking.count").toString());
        BlockingQueue<String> buffer = new ArrayBlockingQueue<String>(bufferSize);    // new LinkedBlockingQueue<>();
        try {
            Thread generator = new DataGenerator(conf, buffer );
            generator.start();
            Thread bufferReader = new BufferReader(buffer, conf, out, serverSocket);
            bufferReader.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class BufferReader extends Thread {
    private BlockingQueue<String> buffer;
    private Logger logger = Logger.getLogger("MyLog");
    private PrintWriter out;
    private ServerSocket serverSocket;
    private int benchmarkCount;
    private HashMap<Long,Integer> thoughputCount = new HashMap<>();
    public BufferReader(BlockingQueue<String> buffer, HashMap conf, PrintWriter out, ServerSocket serverSocket) {
        this.buffer = buffer;
        this.out = out;
        this.serverSocket = serverSocket;
        this.benchmarkCount = new Integer(conf.get("benchmarking.count").toString());
    }

    public void run() {
        try {
            long timeStart = System.currentTimeMillis();

            int tempVal = 0;
            for (int i = 0; i < benchmarkCount; i++) {
                String tuple = buffer.take();
                out.println(tuple);
                if (i % 1000 == 0 ){
                    thoughputCount.put(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), i - tempVal);
                    tempVal = i;
                    logger.info(i + " tuples sent from buffer");
                }
            }
            long timeEnd = System.currentTimeMillis();
            long runtime = (timeEnd - timeStart) / 1000;
            long throughput = benchmarkCount / runtime;

            logger.info("---BENCHMARK ENDED--- on " + runtime + " seconds with " + throughput + " throughput "
                    + " node : " + InetAddress.getLocalHost().getHostName());
            logger.info("Waiting for client on port " + serverSocket.getLocalPort() + "...");
            Socket server = serverSocket.accept();


        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static void writeHashMapToCsv(HashMap<Long, Integer> hm, String path)  {
        try{
            File file = new File(path.split("\\.")[0]+ "-" + InetAddress.getLocalHost().getHostName() + ".csv");

            if (file.exists()) {
                file.delete(); //you might want to check if delete was successfull
            }
            file.createNewFile();
            FileOutputStream fileOutput = new FileOutputStream(file);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fileOutput));
            Iterator it = hm.entrySet().iterator();
            while (it.hasNext()) {
                HashMap.Entry pair = (HashMap.Entry)it.next();
                bw.write(pair.getKey()+ "," + pair.getValue() + "\n");
            }
            bw.flush();
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}


