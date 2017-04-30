import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by mustafa on 22.04.17.
 */
public class ColumnStore implements Serializable {
    enum DT {
        Int, Str, Fl
    }

    public static JavaSparkContext sparkContext;
    private HashMap<String, Integer> nameToIndex = new HashMap<>();
    private String[] schemaNames;

    private DT[] schemaTypes;

    private int numAttrs;

    private ArrayList<JavaRDD<String>> columns = new ArrayList<>();

    public static void main(String[] args) throws IOException, URISyntaxException {
        SparkConf sparkConf = new SparkConf().setAppName("Simple App");

        sparkConf.setMaster("local[4]");

        sparkContext = new JavaSparkContext(sparkConf);

        ColumnStore cs = new ColumnStore();
        cs.run(args[0], args[1], args[2], args[3], args[4]);
    }

    public void run(String inputDataset, String outputAddress, String schema, String projection, String whereclause) throws IOException, URISyntaxException {

        String tempOutputAddress = outputAddress + ".tmp";
        parseSchema(schema);
        loadData(inputDataset);
        JavaRDD<Long> filteredIndices = filterWhere(whereclause);
        JavaPairRDD<Long, String> sortedProjected = joinSorted(filteredIndices, projection);

        FileUtil.fullyDelete(new File(outputAddress));
        FileUtil.fullyDelete(new File(tempOutputAddress));
        sortedProjected.map(new Function<Tuple2<Long, String>, String>() {
            @Override
            public String call(Tuple2<Long, String> longStringTuple2) throws Exception {
                if (longStringTuple2._2.startsWith(",")) {
                    return longStringTuple2._2.substring(1);
                } else {
                    return longStringTuple2._2;
                }
            }
        }).saveAsTextFile(tempOutputAddress);
        Configuration hadoopConfig = new Configuration();
        FileSystem hdfs = FileSystem.get(hadoopConfig);

        FileUtil.copyMerge(hdfs, new Path(tempOutputAddress), hdfs, new Path(outputAddress), true, hadoopConfig, null);
    }

    public JavaPairRDD<Long, String> joinSorted(JavaRDD<Long> indices, String projection) {
        String attrs[] = projection.split(",");
        JavaPairRDD<Long, String> results = indices.mapToPair(new PairFunction<Long, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Long aLong) throws Exception {
                return new Tuple2<>(aLong, "");
            }
        });
        for (String attr : attrs) {
            JavaRDD<String> col = columns.get(nameToIndex.get(attr));

            JavaPairRDD<Long, String> temp = col.zipWithIndex().mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
                @Override
                public Tuple2<Long, String> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                    return new Tuple2<>(stringLongTuple2._2, stringLongTuple2._1);
                }
            });
            results = results.join(temp).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, String>>, Long, String>() {
                @Override
                public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, String>> longTuple2Tuple2) throws Exception {

                    return new Tuple2<>(longTuple2Tuple2._1, longTuple2Tuple2._2._1 + "," + longTuple2Tuple2._2._2);
                }
            });
        }
        return results.sortByKey();
    }

    public JavaRDD<Long> filterWhere(String query) {
        String[] clauses = query.split(",");
        JavaRDD<Long> results = null;
        for (String clause : clauses) {
            String[] parts = clause.split("\\|");
            final String attr = parts[0];
            final String op = parts[1];
            final String val = parts[2];
            JavaRDD<Long> temp = columns.get(nameToIndex.get(attr)).zipWithIndex().filter(new Function<Tuple2<String, Long>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                    return opWithType(stringLongTuple2._1, val, op, schemaTypes[nameToIndex.get(attr)]);
                }
            }).map(new Function<Tuple2<String, Long>, Long>() {
                @Override
                public Long call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                    return stringLongTuple2._2;
                }
            });
            if (results == null) {
                results = temp;
            } else {
                results = results.intersection(temp);
            }
        }
        return results;
    }

    public void parseSchema(String schema) throws IOException, URISyntaxException {
        int typeIndex = 0;
        String[] attrs = schema.split(",");
        numAttrs = attrs.length;
        schemaNames = new String[numAttrs];
        schemaTypes = new DT[numAttrs];
        for (String attr : attrs) {
            String parts[] = attr.split(":");
            schemaNames[typeIndex] = parts[0];
            nameToIndex.put(parts[0], typeIndex);
            switch (parts[1]) {
                case "Int":
                    schemaTypes[typeIndex] = DT.Int;
                    break;
                case "String":
                    schemaTypes[typeIndex] = DT.Str;
                    break;
                case "Fl":
                    schemaTypes[typeIndex] = DT.Fl;
            }
            typeIndex++;
        }
    }

    public void loadData(String addr) throws URISyntaxException, IOException {

        JavaRDD<String> allData = sparkContext.textFile(ColumnStore.class.getResource(addr).getPath());

        for (int i = 0; i < numAttrs; i++) {
            final int ind = i;
            columns.add(allData.map(new Function<String, String>() {
                @Override
                public String call(String s) throws Exception {
                    return s.split(",")[ind];
                }
            }));
        }

        System.out.println(columns.get(0).count());

    }

    private static boolean opWithType(String a, String b, String op, DT type) {
        switch (op) {
            case "=":
                return compareWithType(a, b, type) == 0;
            case "<":
                return compareWithType(a, b, type) > 0;
            case ">":
                return compareWithType(a, b, type) < 0;
            case "<=":
                return compareWithType(a, b, type) >= 0;
            case ">=":
                return compareWithType(a, b, type) <= 0;
            default:
                assert false;
                return false;
        }
    }

    private static long compareWithType(String a, String b, DT type) {
        if (type == DT.Str) {
            return b.compareTo(a);
        } else if (type == DT.Int) {
            return Integer.parseInt(b) - Integer.parseInt(a);
        } else {
            double ad = Double.parseDouble(a);
            double bd = Double.parseDouble(b);
            if (ad == bd) {
                return 0;
            } else if (bd - ad > 0) {
                return 1;
            } else {
                return -1;
            }
        }
    }
}
