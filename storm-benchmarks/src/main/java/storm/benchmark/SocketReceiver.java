package storm.benchmark;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import org.apache.storm.utils.Utils;

/**
 * Created by jeka01 on 03/09/16.
 */

public class SocketReceiver extends BaseRichSpout {
    //The O/P collector
    private SpoutOutputCollector collector;
    //The socket
    private Socket clientSocket;
    private int port;
    private String hostname;
    private BufferedReader in;
    public SocketReceiver(String hostname,int port){
        this.port = port;
        this.hostname =  hostname;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        this.collector=collector;
        try {
            this.clientSocket = new Socket(hostname , port);
            InputStream inFromServer = clientSocket.getInputStream();
            DataInputStream reader = new DataInputStream(inFromServer);
            in = new BufferedReader(new InputStreamReader(reader, "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void nextTuple(){
        try {
            String jsonStr  = in.readLine();
	 //  if( jsonStr == null ){	
	   //   collector.emit(new Values("null"));
	  // }
	   // else {
		
	      collector.emit(new Values(jsonStr));
	  // } 

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("json_string"));
    }
}
