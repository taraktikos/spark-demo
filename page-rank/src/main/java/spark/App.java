package spark;


import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class App {
    private static final Pattern SPACES = Pattern.compile("\\s+");

    public static void main(String[] args) {
        if (args.length < 2) {
            args = new String[]{
                    System.class.getResource("/input.txt").getFile(),
                    "10"
            };
        }
        SparkSession spark = SparkSession.builder()
                .appName("PageRank")
                .master("local[2]")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        JavaPairRDD<String, Iterable<String>> links = lines
                .mapToPair(s -> {
                    String[] parts = SPACES.split(s);
                    return new Tuple2<>(parts[0], parts[1]);
                })
                .distinct()
                .groupByKey()
                .cache();

        // Initialize ranks of all links to one.
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        for (int current = 0; current < Integer.parseInt(args[1]); current++) {
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(tuple -> {
                        int urlCount = Iterables.size(tuple._1);
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : tuple._1) {
                            results.add(new Tuple2<>(n, tuple._2() / urlCount));
                        }
                        return results.iterator();
                    });

            ranks = contribs
                    .reduceByKey((a, b) -> a + b)
                    .mapValues(sum -> 0.15 + sum * 0.85);
        }

        List<Tuple2<String, Double>> output = ranks.collect();
        output.forEach(tuple -> System.out.println(format("+++++++++++++++++++++++ %s has rank %s.", tuple._1(), tuple._2())));

        spark.stop();
    }
}
