import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class simpletest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Testing small ds")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> movieRDD = sc.textFile("movies21.csv");

        JavaRDD<String> ratingRDD = sc.textFile("ratings21.csv");
        String header = movieRDD.first();
        System.out.println(header);

        JavaPairRDD<String, String> moviePairRDD = movieRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String movieid = tokens[0];
                        String title = tokens[1];
                        //String genres = tokens[2];
                        //String info = title + " " + genres;
                        return new Tuple2<String, String>(movieid, title);
                    }
                }
        );
        JavaPairRDD<String, String> ratingPairRDD = ratingRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String movieid = tokens[1];
                        //String userid = tokens[0];
                        String rating = tokens[2];
                        //String otherinfo = userid + " " + rating;
                        return new Tuple2<String, String>(movieid, rating);
                    }
                }
        );


        JavaPairRDD<String, Tuple2<String, String>> joinedPairRDD = moviePairRDD.join(ratingPairRDD);

        JavaPairRDD<String, Tuple2<Float, Integer>> sumCntRDD = joinedPairRDD.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, String>>, String, Tuple2<Float, Integer>>() {
                    public Tuple2<String, Tuple2<Float, Integer>> call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
                        String title = stringTuple2Tuple2._2._1;
                        Float rate = new Float(stringTuple2Tuple2._2._2);
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
        avgRDD.saveAsTextFile("output/test");

    }
}
