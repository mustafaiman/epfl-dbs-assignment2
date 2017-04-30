import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Created by mustafa on 22.04.17.
 */
public class Task4 implements Serializable {

    public static JavaSparkContext sparkContext;

    private final String pathInputCustomer;
    private final String pathInputOrders;
    private final String pathOutput;

    private final int NUM_PARTITIONS = 1024;

    JavaRDD<Long> customerCustKey;
    JavaPairRDD<Long, String> orderCustKeyComment;


    public static void main(String[] args) throws IOException, URISyntaxException {
        SparkConf sparkConf = new SparkConf().setAppName("Simple App");

        ///TODO remove this before submission
        sparkConf.setMaster("local[4]");
        /////////////////////////////

        sparkContext = new JavaSparkContext(sparkConf);

        Task4 t = new Task4(args[0], args[1], args[2]);
        t.distributedJoin();

    }

    public Task4(final String pic, final String pio, final String po) {
        this.pathInputCustomer = pic;
        this.pathInputOrders = pio;
        this.pathOutput = po;

        loadData();


    }

    public void distributedJoin() throws IOException {
        JavaPairRDD<Long, String> dict = customerCustKey.mapToPair(new PairFunction<Long, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Long aLong) throws Exception {
                return new Tuple2<>(aLong, null);
            }
        });


        String tempOutputAddress = "tempTask4Outp";
        dict.union(orderCustKeyComment).partitionBy(new Partitioner() {
            @Override
            public int numPartitions() {
                return NUM_PARTITIONS;
            }

            @Override
            public int getPartition(Object o) {
                return ((Long)o).hashCode() % NUM_PARTITIONS;
            }
        }).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Long,String>>, Long, String>() {
            @Override
            public Iterator<Tuple2<Long, String>> call(Iterator<Tuple2<Long, String>> tuple2Iterator) throws Exception {
                final ArrayList<Tuple2<Long, String>> elements = new ArrayList<>();
                final ArrayList<Tuple2<Long, String>> results = new ArrayList<>();
                final HashMap<Long, Integer> keys = new HashMap<>();

                tuple2Iterator.forEachRemaining(new Consumer<Tuple2<Long, String>>() {
                    @Override
                    public void accept(Tuple2<Long, String> longStringTuple2) {
                        if (longStringTuple2._2 != null) {
                            elements.add(longStringTuple2);
                        } else {
                            Integer oldVal = keys.get(longStringTuple2._1);
                            if ( oldVal == null) {
                                keys.put(longStringTuple2._1, 1);
                            } else {
                                keys.put(longStringTuple2._1, oldVal+1);
                            }
                        }
                    }
                });



                for (int i = 0; i < elements.size(); i++) {
                    Integer appearsInCustomer = keys.get(elements.get(i)._1);
                    if (appearsInCustomer != null) {
                        for (int k = 0; k < appearsInCustomer; k++) {
                            results.add(elements.get(i));
                        }
                    }
                }
                return results.iterator();
            }
        }).map(new Function<Tuple2<Long,String>, String>() {
            @Override
            public String call(Tuple2<Long, String> longStringTuple2) throws Exception {
                return "" + longStringTuple2._1 + "," + longStringTuple2._2;
            }
        }).saveAsTextFile(tempOutputAddress);

        Configuration hadoopConfig = new Configuration();
        FileSystem hdfs = FileSystem.get(hadoopConfig);


        FileUtil.copyMerge(hdfs, new Path(tempOutputAddress), hdfs, new Path(this.pathOutput), true, hadoopConfig, null);
    }

    public void loadData() {
        JavaRDD<String> customerRelation = sparkContext.textFile(Task4.class.getResource(this.pathInputCustomer).getPath());
        JavaRDD<String> ordersRelation = sparkContext.textFile(Task4.class.getResource(this.pathInputOrders).getPath());

        customerCustKey = customerRelation.map(new Function<String, Long>() {
            @Override
            public Long call(String s) throws Exception {
                return Long.parseLong(s.split("\\|")[0]);
            }
        });

        orderCustKeyComment = ordersRelation.mapToPair(new PairFunction<String, Long, String>() {
            @Override
            public Tuple2<Long, String> call(String s) throws Exception {
                String[] parts = s.split("\\|");
                long custKey = Long.parseLong(parts[1]);
                String comment = parts[8];
                return new Tuple2<>(custKey, comment);
            }
        });
    }

    public void run() {


    }


}
