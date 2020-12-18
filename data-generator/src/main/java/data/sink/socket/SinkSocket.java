package data.sink.socket;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import data.sink.model.CalculateLatency;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

public class SinkSocket extends Thread {
    private ServerSocket serverSocket;

    public SinkSocket(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(100000);
    }

    public void run() {
        while (true) {
            try {
                System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
                Socket client = serverSocket.accept();
                System.out.println("Just connected to " + client.getRemoteSocketAddress());
                InputStreamReader inputstreamreader = new InputStreamReader(client.getInputStream());
                BufferedReader bufferedreader = new BufferedReader(inputstreamreader);

                CalculateLatency latencyCalculator = new CalculateLatency();
                long start = System.currentTimeMillis();
                long end = start + 2*1000; // 10 seconds * 1000 ms/sec
                while (System.currentTimeMillis() < end)
                {
                    String s = bufferedreader.readLine();
                    latencyCalculator.insertToModel(Integer.parseInt(s));
                }
                latencyCalculator.printResults();
                System.exit(1);

            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
    }

    public static void main(String[] args) throws YamlException, FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Configuration file argument is needed");
            return;
        }
        String confPath = args[0];
        YamlReader reader = new YamlReader(new FileReader(confPath));
        Object object = reader.read();
        Map map = (Map) object;

        int port = new Integer(map.get("datasinksocket.port").toString());
        try {
            Thread t = new SinkSocket(port);
            t.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
