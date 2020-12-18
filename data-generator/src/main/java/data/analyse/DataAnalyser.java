package data.analyse;

import org.streaminer.stream.quantile.Frugal2U;
import org.streaminer.stream.quantile.IQuantiles;
import org.streaminer.stream.quantile.QuantilesException;
import org.streaminer.stream.quantile.WindowSketchQuantiles;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by jeka01 on 06/09/16.
 */
public class DataAnalyser {

    private BufferedReader br = null;
    private  double[] quantiles;
    private IQuantiles<Integer> instance;

    private DataAnalyser(String inputFilePath, double[] quantiles) throws FileNotFoundException {
        br = new BufferedReader(new FileReader(inputFilePath));
        this.quantiles = quantiles;
    }

    public static void analyse(String inputFilePath,double[] quantiles) throws IOException, QuantilesException {
        DataAnalyser analyser = new DataAnalyser(inputFilePath, quantiles);
        analyser.insertValues();
        analyser.printQuantiles();
    }
    private void printQuantiles() throws QuantilesException {
        for (double q : quantiles) {
            System.out.println(q + ": " + instance.getQuantile(q));
        }

    }

    private void insertValues() throws QuantilesException, IOException {
        instance = new Frugal2U(quantiles, 0);
        String sCurrentLine;
        while ((sCurrentLine = br.readLine()) != null) {
            Integer i = new Integer(sCurrentLine);
            instance.offer(i);
        }
    }



    public static void main(String[] args) throws IOException, QuantilesException {
        DataAnalyser.analyse("/Users/jeka01/Documents/workspaces/benchmarking/streaming-benchmarks-master/output/flink/flink-8000-2000.txt",
                new double[]{0.05, 0.25, 0.5, 0.75, 0.95});
    }
}
