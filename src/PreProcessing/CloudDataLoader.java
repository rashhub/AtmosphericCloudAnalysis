package PreProcessing;

import App.Application;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.deploy.master.ApplicationState;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.io.*;

public class CloudDataLoader implements Serializable {


    private String land_data_file_path;
    private String ocean_data_file_path;


    public CloudDataLoader(String land_data_file_path, String ocean_data_file_path) {
        this.land_data_file_path = land_data_file_path;
        this.ocean_data_file_path = ocean_data_file_path;
    }

    // The argument to the main function is the input file name
    // (specified as a parameter to the spark-submit command)
    public JavaRDD<String> load_land_data(Application as) throws Exception {

        // Create a JavaRDD of strings; each string is a line read from
        // a text file.
        JavaRDD<String> land_data_lines = as.getSparkContext().textFile(this.land_data_file_path);

        JavaRDD<String> land_raw_data_rdd = land_data_lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {

                String year = s.substring(0,2);  //Year
                String mon = s.substring(2,4);  //Month
                String day = s.substring(4,6);  //Day
                String hr = s.substring(6,8);  //Hour

                String brightness = String.valueOf(s.charAt(8));  //Brightness indicator

                String lat = s.substring(9,14).trim(); //Latitude
                String lon = s.substring(14,19).trim();; //longitude

                String stn_nbr = s.substring(19,24).trim();;  //Station Number

                String land_ocean_flg = String.valueOf(s.charAt(24)).trim();; // land and Ocean indicator

                String p_ww = s.substring(25,27).trim();; //present weather

                String total_cc = String.valueOf(s.charAt(27)); //Total Cloud Cover

                String lwr_cloud_amt = s.substring(28,30).trim();  //Lower cloud amount

                String lwr_cld_base_height = s.substring(30,32).trim(); //Lower Cloud Base Height

                String low_cloud_type = s.substring(32,34).trim(); //Low cloud type

                String mid_cloud_type = s.substring(34,36).trim(); //middle cloud type

                String high_cloud_type = s.substring(36,38).trim();  //high cloud type

                String mid_cloud_amt = s.substring(38,41).trim();
                String high_cloud_amt = s.substring(41,44).trim();

                String mid_cld_amt = String.valueOf(s.charAt(44)); //Total Cloud Cover
                String high_cld_amt = String.valueOf(s.charAt(45)); //Total Cloud Cover

                String change_code = s.substring(46,48).trim(); //Change code

                String solar_alt = s.substring(48,52).trim(); //Solar Altitude
                String lunar_illum = s.substring(52,55).concat(String.valueOf(s.charAt(55))).trim(); //relative Lunar illuminance


                String land_obs_data =year.concat(",").concat(mon).concat(",").concat(day).concat(",").concat(hr).concat(",")
                                            .concat(brightness).concat(",").concat(lat).concat(",").concat(lon).concat(",")
                                            .concat(stn_nbr).concat(",").concat(land_ocean_flg).concat(",").concat(p_ww).concat(",")
                                            .concat(total_cc).concat(",").concat(lwr_cld_base_height).concat(",").concat(lwr_cloud_amt)
                                            .concat(",").concat(mid_cloud_amt).concat(",").concat(high_cloud_amt).concat(",").concat(low_cloud_type)
                                            .concat(",").concat(mid_cloud_type).concat(",").concat(high_cloud_type).concat(",").concat(solar_alt)
                                            .concat(",").concat(lunar_illum).concat(",").concat(change_code);

                return land_obs_data;
            }
        });


        return land_raw_data_rdd;

    }

    public JavaRDD<String> load_ocean_data(Application as) throws Exception {

        JavaRDD<String> ocean_data_lines = as.getSparkContext().textFile(this.ocean_data_file_path);

        JavaRDD<String> ocean_raw_data_rdd = ocean_data_lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {

                char[] str= s.toCharArray();


                String year = s.substring(0,2);  //Year
                String mon = s.substring(2,4);  //Month
                String day = s.substring(4,6);  //Day
                String hr = s.substring(6,8);  //Hour

                String brightness = String.valueOf(s.charAt(8));  //Brightness indicator

                String lat = s.substring(9,14).trim(); //Latitude
                String lon = s.substring(14,19).trim(); //longitude

                String stn_nbr = s.substring(19,24).trim();  //Station Number

                String land_ocean_flg = String.valueOf(s.charAt(24)); // land and Ocean indicator

                String p_ww = s.substring(25,27).trim(); //present weather

                String total_cc = String.valueOf(s.charAt(27)); //Total Cloud Cover

                String lwr_cloud_amt = s.substring(28,30).trim();  //Lower cloud amount

                String lwr_cld_base_height = s.substring(30,32).trim(); //Lower Cloud Base Height

                String low_cloud_type = s.substring(32,34).trim(); //Low cloud type

                String mid_cloud_type = s.substring(34,36).trim(); //middle cloud type

                String high_cloud_type = s.substring(36,38).trim();  //high cloud type

                String mid_cloud_amt = s.substring(38,41).trim();
                String high_cloud_amt = s.substring(41,44).trim();

                String mid_cld_amt = String.valueOf(s.charAt(44)); //Total Cloud Cover
                String high_cld_amt = String.valueOf(s.charAt(45)); //Total Cloud Cover

                String change_code = s.substring(46,48).trim(); //Change code

                String solar_alt = s.substring(48,52).trim(); //Solar Altitude
                String lunar_illum = s.substring(52,55).concat(String.valueOf(s.charAt(55))).trim(); //relative Lunar illuminance


                String ocean_obs_data =year.concat(",").concat(mon).concat(",").concat(day).concat(",").concat(hr).concat(",")
                        .concat(brightness).concat(",").concat(lat).concat(",").concat(lon).concat(",")
                        .concat(stn_nbr).concat(",").concat(land_ocean_flg).concat(",").concat(p_ww).concat(",")
                        .concat(total_cc).concat(",").concat(lwr_cld_base_height).concat(",").concat(lwr_cloud_amt)
                        .concat(",").concat(mid_cloud_amt).concat(",").concat(high_cloud_amt).concat(",").concat(low_cloud_type)
                        .concat(",").concat(mid_cloud_type).concat(",").concat(high_cloud_type).concat(",").concat(solar_alt)
                        .concat(",").concat(lunar_illum).concat(",").concat(change_code);

                return ocean_obs_data;
            }
        });


        return ocean_raw_data_rdd;
    }


}
