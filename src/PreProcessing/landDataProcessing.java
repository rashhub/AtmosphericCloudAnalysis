package PreProcessing;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.io.*;

public final class landDataProcessing {

    // The argument to the main function is the input file name
    // (specified as a parameter to the spark-submit command)
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: Land Data Pre-Processing <file>");
            System.exit(1);
        }

        // Create a new Spark Context
        SparkConf conf = new SparkConf().setAppName("landDataProcessing").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a JavaRDD of strings; each string is a line read from
        // a text file.
        JavaRDD<String> land_data_lines = sc.textFile(args[0]);

        JavaRDD<String> all_fields_data = land_data_lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {

                String year = s.substring(0,2);  //Year
                String mon = s.substring(2,4);  //Month
                String day = s.substring(4,6);  //Day
                String hr = s.substring(6,8);  //Hour

                String brightness = String.valueOf(s.charAt(8));  //Brightness indicator

                String lat = s.substring(9,14); //Latitude
                String lon = s.substring(14,19); //longitude

                String stn_nbr = s.substring(19,24);  //Station Number

                String land_ocean = String.valueOf(s.charAt(24)); // land and Ocean indicator

                String p_ww = s.substring(25,27); //present weather

                String total_cc = String.valueOf(s.charAt(27)); //Total Cloud Cover

                String lwr_cloud_amt = s.substring(28,30);  //Lower cloud amount

                String lwr_cld_base_height = s.substring(30,32); //Lower Cloud Base Height

                String low_cloud_type = s.substring(32,34); //Low cloud type

                String mid_cloud_type = s.substring(34,36); //middle cloud type

                String high_cloud_type = s.substring(36,38);  //high cloud type

                String mid_cloud_amt = s.substring(38,41);
                String high_cloud_amt = s.substring(41,44);

                String mid_cld_amt = String.valueOf(s.charAt(44)); //Total Cloud Cover
                String high_cld_amt = String.valueOf(s.charAt(45)); //Total Cloud Cover

                String change_code = s.substring(46,48); //Change code

                String solar_alt = s.substring(48,52); //Solar Altitude
                String lunar_illum = s.substring(52,55).concat(String.valueOf(s.charAt(55))); //relative Lunar illuminance


                String cloud_land_obs_data =year.concat(",").concat(mon).concat(",").concat(day).concat(",").concat(hr).concat(",")
                                            .concat(brightness).concat(",").concat(lat).concat(",").concat(lon).concat(",")
                                            .concat(stn_nbr).concat(",").concat(land_ocean).concat(",").concat(p_ww).concat(",")
                                            .concat(total_cc).concat(",").concat(lwr_cld_base_height).concat(",").concat(lwr_cloud_amt)
                                            .concat(",").concat(mid_cloud_amt).concat(",").concat(high_cloud_amt).concat(",").concat(low_cloud_type)
                                            .concat(",").concat(mid_cloud_type).concat(",").concat(high_cloud_type).concat(",").concat(solar_alt)
                                            .concat(",").concat(lunar_illum).concat(",").concat(change_code);

                return cloud_land_obs_data;
            }
        });


        System.out.println(all_fields_data.count());


    }
}
