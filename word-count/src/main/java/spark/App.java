package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.util.Arrays.asList;

public class App {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            args = new String[]{
                    System.class.getResource("/input.txt").getFile(),
                    "/tmp/spark-demo-output/"
            };
        }
        Path path = Paths.get(args[1]);
        Files.walk(path).map(Path::toFile).forEach(File::delete);
        Files.deleteIfExists(path);

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Spark demo");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> file = context.textFile(args[0]);
        JavaRDD<String> words = file.flatMap(s -> asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w, 1));
        JavaPairRDD<String, Integer> counter = pairs.reduceByKey((a, b) -> a + b);
        counter.saveAsTextFile(args[1]);
    }
}
