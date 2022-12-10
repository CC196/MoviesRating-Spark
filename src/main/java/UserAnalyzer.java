import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Timestamp;
import java.util.Date;

public class UserAnalyzer {
    public static void getUserAnalyzer(String SelectUserID) {

        //Integer SelectUserID = new Integer("99");
        SparkConf sparkConf = new SparkConf()
                .setAppName("User Analysis")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> ratingRDD = sparkContext.textFile("ratings.csv");
        JavaRDD<String> tagsRDD = sparkContext.textFile("tags.csv");
        JavaRDD<String> movieRDD = sparkContext.textFile("movies.csv");

        JavaRDD<String> SelectUserRDD = ratingRDD.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        Integer UserID = new Integer(tokens[0]).intValue();
                        if (UserID.equals(SelectUserID)) return true;
                        else return false;
                    }
                }
        );

        JavaRDD<String> SUtagsRDD = tagsRDD.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        Integer UserID = new Integer(tokens[0]).intValue();
                        if (UserID.equals(SelectUserID)) return true;
                        else return false;
                    }
                }
        );
        if(SUtagsRDD.isEmpty())System.out.println("No Tags");

        JavaPairRDD<String, Tuple2< String, Date>> RURDD = SelectUserRDD.mapToPair(
                new PairFunction<String, String, Tuple2< String, Date>>() {
                    public Tuple2<String, Tuple2< String, Date>> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String UserID = tokens[0];
                        String MovieID = tokens[1];
                        String rate = tokens[2];
                        Long st_t = Long.parseLong(tokens[3]);
                        Date date=new Date(st_t*1000);

                        return new Tuple2(MovieID, new Tuple2(rate, date));
                    }
                }
        );

        JavaPairRDD<String, Tuple2< String, Date>> TURDD = SUtagsRDD.mapToPair(
                new PairFunction<String, String, Tuple2< String, Date>>() {
                    public Tuple2<String, Tuple2< String, Date>> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String UserID = tokens[0];
                        String MovieID = tokens[1];
                        String tag = tokens[2];
                        Long st_t = Long.parseLong(tokens[3]);
                        Date date=new Date(st_t*1000);

                        return new Tuple2(MovieID, new Tuple2(tag, date));
                    }
                }
        );

        JavaPairRDD<String, Tuple2<String,String>> moviePairRDD = movieRDD.mapToPair(
                new PairFunction<String, String, Tuple2<String,String>>() {
                    public Tuple2<String, Tuple2<String,String>> call(String s) throws Exception {
                        String[] tokens = s.split(";");
                        String movieid = tokens[0];
                        String title = tokens[1];
                        String gen = tokens[2];
                        return new Tuple2<String, Tuple2<String,String>>(movieid, new Tuple2(title, gen));
                    }
                }
        );

        JavaPairRDD<String, Tuple2< String, Date>> JoinRDD = RURDD.union(TURDD).repartition(1);
        JavaPairRDD<String, Tuple2<Tuple2<String,Date>, Tuple2<String,String>>> Join2RDD = JoinRDD.join(moviePairRDD);
        JavaPairRDD<Date, Tuple2<String, String>> finalRDD = Join2RDD.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Date>, Tuple2<String, String>>>, Date, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<Date, Tuple2<String, String>> call(Tuple2<String, Tuple2<Tuple2<String, Date>, Tuple2<String, String>>> stringTuple2Tuple2) throws Exception {
                        return new Tuple2(stringTuple2Tuple2._2._1._2, new Tuple2<>(stringTuple2Tuple2._2._2._1, stringTuple2Tuple2._2._1._1));
                    }
                }
        ).sortByKey();
        String savepath = "output/user" + SelectUserID;
        finalRDD.saveAsTextFile(savepath);

    }
}
