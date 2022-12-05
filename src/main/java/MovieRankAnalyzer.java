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
                .setAppName("Movie's Score")
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
        JavaPairRDD<String, Float> ratingPairRDD = ratingRDD.mapToPair(
                new PairFunction<String, String, Float>() {
                    public Tuple2<String, Float> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String movieid = tokens[1];
                        float rating = Float.parseFloat(tokens[2]);
                        return new Tuple2(movieid, rating);
                    }
                }
        );

        JavaPairRDD<String, Tuple2<String, Float>> joinedPairRDD = moviePairRDD.join(ratingPairRDD);

        JavaPairRDD<String, Tuple2<Float, Integer>> sumCntRDD = joinedPairRDD.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Float>>, String, Tuple2<Float, Integer>>() {
                    public Tuple2<String, Tuple2<Float, Integer>> call(Tuple2<String, Tuple2<String, Float>> stringTuple2Tuple2) throws Exception {
                        String title = stringTuple2Tuple2._2._1;
                        Float rate = stringTuple2Tuple2._2._2;
                        return new Tuple2<String, Tuple2<Float, Integer>>(title, new Tuple2(rate, 1));
                    }
                }
        );

        JavaPairRDD<String, Tuple2<Float, Integer>> finalRDD = sumCntRDD.reduceByKey(
                new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
                    public Tuple2<Float, Integer> call(Tuple2<Float, Integer> floatIntegerTuple2, Tuple2<Float, Integer> floatIntegerTuple22) throws Exception {

                        Float scoreSum = floatIntegerTuple2._1 + floatIntegerTuple22._1;
                        Integer countSum = floatIntegerTuple2._2 + floatIntegerTuple22._2;
                        return new Tuple2(scoreSum, countSum);
                    }
                }
        );
        JavaPairRDD<String, Float> avgRDD = finalRDD.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<Float, Integer>>, String, Float>() {
                    public Tuple2<String, Float> call(Tuple2<String, Tuple2<Float, Integer>> stringTuple2Tuple2) throws Exception {
                        Float scoreSum = stringTuple2Tuple2._2._1;
                        Integer countSum = stringTuple2Tuple2._2._2;
                        Float scoreAvg = new Float(scoreSum/countSum);
                        return new Tuple2<String, Float>(stringTuple2Tuple2._1, scoreAvg);
                    }
                }
        );
        avgRDD.saveAsTextFile("output/avg");

    }
}
