package Analysis;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by Rash on 18-04-2017.
 */
public class DiscriptiveStats implements Serializable {
    public DiscriptiveStats() {
    }

    public JavaPairRDD<String,Long> station_count(JavaRDD<String> ship_land_rdd)
    {
        JavaPairRDD<String,Long> stn_count_rdd= ship_land_rdd.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {

                String[] str = s.split(",");



                return new Tuple2<String, Long>(str[7],Long.valueOf(1));
            }
        });


        JavaPairRDD<String,Long> agg_stn_entries_rdd =stn_count_rdd.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a1, Long a2) throws Exception {
                return (a2+a2);
            }
        });



        return agg_stn_entries_rdd;
    }


}
