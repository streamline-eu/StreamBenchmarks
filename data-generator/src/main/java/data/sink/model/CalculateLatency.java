package data.sink.model;

import cern.jet.random.Normal;
import cern.jet.random.engine.MersenneTwister64;
import cern.jet.random.engine.RandomEngine;
import org.jblas.util.Random;
import org.streaminer.stream.quantile.Frugal2U;
import org.streaminer.stream.quantile.IQuantiles;
import org.streaminer.stream.quantile.QuantilesException;

/**
 * Created by jeka01 on 05/09/16.
 */
public class CalculateLatency {
    private double[] quantiles = new double[]{0.05, 0.25, 0.5, 0.75, 0.95};
    private IQuantiles<Integer> instance;
    public CalculateLatency(){
         instance = new Frugal2U(quantiles, 0);
        RandomEngine r = new MersenneTwister64(0);
        Normal dist = new Normal(100, 50, r);

    }

    public  void insertToModel(int val)  {
        instance.offer(val);
    }

    public void printResults(){
        try {
            for (double q : quantiles) {
                System.out.println(q + ": " + instance.getQuantile(q));
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }


//    public static void main(String[] args){
//        CalculateLatency cal = new CalculateLatency();
//        long start = System.currentTimeMillis();
//        long end = start + 10*1000; // 60 seconds * 1000 ms/sec
//        while (System.currentTimeMillis() < end)
//        {
//            int i = cal.randInt(0,5);
//            cal.insertToModel(i);
//        }
//        cal.printResults();
//
//    }
//
//    public  int randInt(int min, int max) {
//
//        // NOTE: This will (intentionally) not run as written so that folks
//        // copy-pasting have to think about how to initialize their
//        // Random instance.  Initialization of the Random instance is outside
//        // the main scope of the question, but some decent options are to have
//        // a field that is initialized once and then re-used as needed or to
//        // use ThreadLocalRandom (if using at least Java 1.7).
//        Random rand = new Random();
//
//        // nextInt is normally exclusive of the top value,
//        // so add 1 to make it inclusive
//        int randomNum = rand.nextInt((max - min) + 1) + min;
//
//        return randomNum;
//    }

}
