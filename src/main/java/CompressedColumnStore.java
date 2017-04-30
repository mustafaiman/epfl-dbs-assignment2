import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
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
public class CompressedColumnStore implements Serializable {
    enum DT {
        Int, Str, Fl
    }

    public static JavaSparkContext sparkContext;
    private HashMap<String, Integer> nameToIndex = new HashMap<>();
    private String[] schemaNames;
    private String compressedColumnName;
    private int compressedColumnId;

    private DT[] schemaTypes;

    private int numAttrs;

    private ArrayList<JavaRDD<String>> columns = new ArrayList<>();
    private JavaPairRDD<String, Long> dictionary;
    private JavaRDD<String> compressedColumn;

    public static void main(String[] args) throws IOException, URISyntaxException {
        SparkConf sparkConf = new SparkConf().setAppName("Simple App");

        sparkConf.setMaster("local[4]");

        sparkContext = new JavaSparkContext(sparkConf);

        CompressedColumnStore cs = new CompressedColumnStore();
        cs.run(args[0], args[1], args[2], args[3], args[4], args[5]);
    }

    public void run(String inputDataset, String outputAddress, String schema, String projection, String whereclause, String compressedColumn) throws IOException, URISyntaxException {

        String tempOutputAddress = outputAddress + ".tmp";
        parseSchema(schema);
        loadData(inputDataset);
        dictionaryEncode(compressedColumn);
        compressColumn(compressedColumn);
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

    public void dictionaryEncode(String column) {
        compressedColumnId = nameToIndex.get(column);
        compressedColumnName = column;
        dictionary = columns.get(nameToIndex.get(column)).distinct().zipWithIndex();
    }

    public void compressColumn(String column) {
        JavaRDD<String> col = columns.get(nameToIndex.get(column));
        compressedColumn = col.zipWithIndex().join(dictionary).mapToPair(new PairFunction<Tuple2<String,Tuple2<Long,Long>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Tuple2<Long, Long>> stringTuple2Tuple2) throws Exception {
                return new Tuple2<>(stringTuple2Tuple2._2._1, ""+stringTuple2Tuple2._2._2);
            }
        }).sortByKey().map(new Function<Tuple2<Long, String>, String>() {
            @Override
            public String call(Tuple2<Long, String> longStringTuple2) throws Exception {
                return longStringTuple2._2;
            }
        });
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
            final String candidateVal = parts[2];
            final String val;
            int attrIndex = nameToIndex.get(attr);
            boolean earlyTerminate = false;

            JavaRDD<String> toOperate;
            if (attrIndex == compressedColumnId && op.equals("=")) {
                JavaPairRDD<String, Long> mapping = dictionary.filter(new Function<Tuple2<String, Long>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return stringLongTuple2._1.equals(candidateVal);
                    }
                });
                if (mapping.isEmpty()) {
                    earlyTerminate = true;
                    val = null;
                } else {
                    val = ""+mapping.first()._2;
                }
                toOperate = compressedColumn;
            } else {
                val = candidateVal;
                toOperate = columns.get(attrIndex);
            }
            JavaRDD<Long> temp = sparkContext.emptyRDD();
            if (!earlyTerminate) {
                temp = toOperate.zipWithIndex().filter(new Function<Tuple2<String, Long>, Boolean>() {
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
            }

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

        JavaRDD<String> allData = sparkContext.textFile(CompressedColumnStore.class.getResource(addr).getPath());

        for (int i = 0; i < numAttrs; i++) {
            final int ind = i;
            columns.add(allData.map(new Function<String, String>() {
                @Override
                public String call(String s) throws Exception {
                    return s.split(",")[ind];
                }
            }));
        }
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
