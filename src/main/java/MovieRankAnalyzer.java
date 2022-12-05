import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MovieRankAnalyzer {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> movieRDD = sparkContext.textFile("movies.csv");
        JavaRDD<String> ratingRDD = sparkContext.textFile("ratings.csv");

        JavaPairRDD<String, String> moviePairRDD = movieRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String movieid = tokens[0];
                        String title = tokens[1];
                        return new Tuple2<String, String>(movieid, title);
                    }
                }
        );
        JavaPairRDD<String, String> ratingPairRDD = ratingRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String movieid = tokens[1];
                        String rating = tokens[2];
                        return new Tuple2<String, String>(movieid, rating);
                    }
                }
        );


        JavaPairRDD<String, Tuple2<String, String>> joinedPairRDD = moviePairRDD.join(ratingPairRDD);

        joinedPairRDD.saveAsTextFile("output/avg");

    }
}
