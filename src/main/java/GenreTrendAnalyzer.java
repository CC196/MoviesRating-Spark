import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Int;
import scala.Tuple2;

import java.util.Calendar;

public class GenreTrendAnalyzer {
    public static void getReport(String movieType){
        SparkConf sparkConf = new SparkConf()
                .setAppName("Movie Trend")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> movieRDD = javaSparkContext.textFile("movies.csv"); // [0] movie id, [2] genre
        JavaRDD<String> ratingRDD = javaSparkContext.textFile("ratings.csv"); // [1] movie id, [3] timestamp

        // filter movies table with user input genre
        JavaRDD<String> selectedMovieTypeRDD = movieRDD.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        String[] token = s.split(";");
                        return token[2].toLowerCase().contains(movieType.toLowerCase());
                    }
                }
        );

        // return if no genre found
        if(selectedMovieTypeRDD.isEmpty()) System.out.println("Genre Not Found!");

        // map movid id and genre from filtered movie table
        JavaPairRDD<String, String> moviePairRDD = selectedMovieTypeRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] tokens = s.split(";");
                        String movieid = tokens[0];
                        return new Tuple2<String, String>(movieid, movieType);
                    }
                }
        );

        // map movie id and time from rating table
        JavaPairRDD<String, String> ratingPairRDD = ratingRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String movieid = tokens[1];
                        Long milliseconds = Long.parseLong(tokens[3]);
                        Calendar c = Calendar.getInstance();
                        c.setTimeInMillis(milliseconds * 1000);
                        int year = c.get(Calendar.YEAR);
                        return new Tuple2(movieid, Integer.toString(year));
                    }
                }
        );

        // join movie and rating table
        JavaPairRDD<String, Tuple2<String, String>> joinedPairRDD = moviePairRDD.join(ratingPairRDD);

        JavaPairRDD<String, Integer> sumCntRDD = joinedPairRDD.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, String>>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
                        String year = stringTuple2Tuple2._2._2;
                        return new Tuple2<String, Integer>(year, 1);
                    }
                }
        );

        JavaPairRDD<String, Integer> finalRDD = sumCntRDD.reduceByKey((v1, v2) -> (v1 + v2)).repartition(5);

        finalRDD.saveAsTextFile("output/genre_trend/" + movieType);
    }
}
