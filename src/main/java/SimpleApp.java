import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Iterator;

/**
 * Created by mustafa on 22.04.17.
 */
public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "/home/mustafa/sample.log";
        SparkConf sparkConf = new SparkConf().setAppName("Simple App");
        sparkConf.setMaster("local[4]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> logData = sparkContext.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter((new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("b");
            }
        })).count();

        System.out.println("As: " + numAs + "\nBs: " + numBs);

        sparkContext.stop();
    }
}
