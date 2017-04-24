package PreProcessing;

import App.Application;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaRDD;

import java.text.DecimalFormat;
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

        JavaRDD<String> land_struct_rdd = land_data_lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {

                if(s.length()==80) {
                    String year = s.substring(0, 2);  //Year
                    String mon = s.substring(2, 4);  //Month
                    String day = s.substring(4, 6);  //Day
                    String hr = s.substring(6, 8);  //Hour

                    String brightness = String.valueOf(s.charAt(8));  //Brightness indicator

                    DecimalFormat df = new DecimalFormat("###.##");

                    float lat_intrim = Float.valueOf(s.substring(9, 14).trim())/100; //Latitude
                    String lat = String.valueOf(df.format(lat_intrim));

                    float lon_intrim = Float.valueOf(s.substring(14, 19).trim())/100; //longitude

                    String lon;

                    if(lon_intrim>180)
                    {
                        lon_intrim = ((lon_intrim+180) % 360) - 180;

                        lon = String.valueOf(df.format(lon_intrim));
                    }else
                    {
                        lon = String.valueOf(df.format(lon_intrim));
                    }


                    String stn_nbr = s.substring(19, 24).trim();  //Station Number

                    String land_ocean_flg = String.valueOf(s.charAt(24)).trim(); // land and Ocean indicator

                    String p_ww = s.substring(25, 27).trim(); //present weather

                    String total_cc = String.valueOf(s.charAt(27)); //Total Cloud Cover

                    String lwr_cloud_amt = s.substring(28, 30).trim();  //Lower cloud amount

                    String lwr_cld_base_height = s.substring(30, 32).trim(); //Lower Cloud Base Height

                    String low_cloud_type = s.substring(32, 34).trim(); //Low cloud type

                    String mid_cloud_type = s.substring(34, 36).trim(); //middle cloud type

                    String high_cloud_type = s.substring(36, 38).trim();  //high cloud type

                    String mid_cloud_amt = String.valueOf(Integer.valueOf(s.substring(38, 41).trim())/100);
                    String high_cloud_amt =String.valueOf((Integer.valueOf(s.substring(41, 44).trim())/100));

                    String mid_cld_amt = String.valueOf(s.charAt(44)); //Total Cloud Cover
                    String high_cld_amt = String.valueOf(s.charAt(45)); //Total Cloud Cover

                    String change_code = s.substring(46, 48).trim(); //Change code

                    String solar_alt = String.valueOf((Integer.valueOf(s.substring(48, 52).trim())/10)); //Solar Altitude
                    String lunar_illum = String.valueOf((Integer.valueOf(s.substring(52, 56).trim())/100)); //relative Lunar illuminance

                    String sea_pressure = String.valueOf((Integer.valueOf(s.substring(56, 61).trim())/10)); //sea pressure level

                    String wind_speed = String.valueOf((Integer.valueOf(s.substring(61, 64).trim())/10));//wind speed

                    String wind_dir = s.substring(64, 67).trim(); //wind direction

                    String air_temp = String.valueOf((Integer.valueOf(s.substring(67, 71).trim())/10));//air temperature

                    String dew_point = String.valueOf((Integer.valueOf(s.substring(71, 74).trim())/10)); //dew point depression

                    String elevation = s.substring(74, 78).trim(); //land elevation

                    String wind_speed_ind = String.valueOf(s.charAt(78)); //

                    String pressure_flag = String.valueOf(s.charAt(79)); //


                    String land_obs_data = year.concat(",").concat(mon).concat(",").concat(day).concat(",").concat(hr).concat(",")
                            .concat(brightness).concat(",").concat(lat).concat(",").concat(lon).concat(",")
                            .concat(stn_nbr).concat(",").concat(land_ocean_flg).concat(",").concat(p_ww).concat(",")
                            .concat(total_cc).concat(",").concat(lwr_cld_base_height).concat(",").concat(lwr_cloud_amt)
                            .concat(",").concat(mid_cloud_amt).concat(",").concat(high_cloud_amt).concat(",").concat(low_cloud_type)
                            .concat(",").concat(mid_cloud_type).concat(",").concat(high_cloud_type).concat(",").concat(solar_alt)
                            .concat(",").concat(lunar_illum).concat(",").concat(change_code).concat(",").concat(sea_pressure).concat(",")
                            .concat(wind_speed).concat(",").concat(wind_dir).concat(",").concat(air_temp).concat(",").concat(dew_point)
                            .concat(",").concat(elevation);

                    return land_obs_data;
                }else
                {
                    return null;
                }
            }
        });

        JavaRDD<String> land_raw_data_rdd= land_struct_rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if(s==null) {
                    return false;
                }else
                {
                    return true;
                }
            }
        });

        return land_raw_data_rdd;

    }

    public JavaRDD<String> load_ocean_data(Application as) throws Exception {

        JavaRDD<String> ocean_data_lines = as.getSparkContext().textFile(this.ocean_data_file_path);

        JavaRDD<String> ocean_struct_rdd = ocean_data_lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {

                if(s.length()==80) {

                    String year = s.substring(0, 2);  //Year
                    String mon = s.substring(2, 4);  //Month
                    String day = s.substring(4, 6);  //Day
                    String hr = s.substring(6, 8);  //Hour

                    String brightness = String.valueOf(s.charAt(8));  //Brightness indicator

                    DecimalFormat df = new DecimalFormat("###.##");

                    float lat_intrim = Float.valueOf(s.substring(9, 14).trim())/100; //Latitude
                    String lat = String.valueOf(df.format(lat_intrim));

                    float lon_intrim = Float.valueOf(s.substring(14, 19).trim())/100; //longitude

                    String lon;

                    if(lon_intrim>180)
                    {
                        lon_intrim = ((lon_intrim+180) % 360) - 180;

                        lon = String.valueOf(df.format(lon_intrim));
                    }else
                    {
                        lon = String.valueOf(df.format(lon_intrim));
                    }


                    String stn_nbr = s.substring(19, 24).trim();  //Station Number

                    String land_ocean_flg = String.valueOf(s.charAt(24)).trim(); // land and Ocean indicator

                    String p_ww = s.substring(25, 27).trim(); //present weather

                    String total_cc = String.valueOf(s.charAt(27)); //Total Cloud Cover

                    String lwr_cloud_amt = s.substring(28, 30).trim();  //Lower cloud amount

                    String lwr_cld_base_height = s.substring(30, 32).trim(); //Lower Cloud Base Height

                    String low_cloud_type = s.substring(32, 34).trim(); //Low cloud type

                    String mid_cloud_type = s.substring(34, 36).trim(); //middle cloud type

                    String high_cloud_type = s.substring(36, 38).trim();  //high cloud type

                    String mid_cloud_amt = String.valueOf(Integer.valueOf(s.substring(38, 41).trim())/100);
                    String high_cloud_amt =String.valueOf((Integer.valueOf(s.substring(41, 44).trim())/100));

                    String mid_cld_amt = String.valueOf(s.charAt(44)); //Total Cloud Cover
                    String high_cld_amt = String.valueOf(s.charAt(45)); //Total Cloud Cover

                    String change_code = s.substring(46, 48).trim(); //Change code

                    String solar_alt = String.valueOf((Integer.valueOf(s.substring(48, 52).trim())/10)); //Solar Altitude
                    String lunar_illum = String.valueOf((Integer.valueOf(s.substring(52, 56).trim())/100)); //relative Lunar illuminance

                    String sea_pressure = String.valueOf((Integer.valueOf(s.substring(56, 61).trim())/10)); //sea pressure level

                    String wind_speed = String.valueOf((Integer.valueOf(s.substring(61, 64).trim())/10));//wind speed

                    String wind_dir = s.substring(64, 67).trim(); //wind direction

                    String air_temp = String.valueOf((Integer.valueOf(s.substring(67, 71).trim())/10));//air temperature

                    String dew_point = String.valueOf((Integer.valueOf(s.substring(71, 74).trim())/10)); //dew point depression

                    String sea_surface_temp = String.valueOf((Integer.valueOf(s.substring(74, 78).trim())/10)); //Sea Surface temperature

                    String wind_speed_ind = String.valueOf(s.charAt(78)); //

                    String pressure_flag = String.valueOf(s.charAt(79)); //


                    String ocean_obs_data = year.concat(",").concat(mon).concat(",").concat(day).concat(",").concat(hr).concat(",")
                            .concat(brightness).concat(",").concat(lat).concat(",").concat(lon).concat(",")
                            .concat(stn_nbr).concat(",").concat(land_ocean_flg).concat(",").concat(p_ww).concat(",")
                            .concat(total_cc).concat(",").concat(lwr_cld_base_height).concat(",").concat(lwr_cloud_amt)
                            .concat(",").concat(mid_cloud_amt).concat(",").concat(high_cloud_amt).concat(",").concat(low_cloud_type)
                            .concat(",").concat(mid_cloud_type).concat(",").concat(high_cloud_type).concat(",").concat(solar_alt)
                            .concat(",").concat(lunar_illum).concat(",").concat(change_code).concat(",").concat(sea_pressure).concat(",")
                            .concat(wind_speed).concat(",").concat(wind_dir).concat(",").concat(air_temp).concat(",").concat(dew_point)
                            .concat(",").concat(sea_surface_temp);

                    return ocean_obs_data;
                }else
                {
                    return null;
                }
            }
        });


        JavaRDD<String> ocean_raw_data_rdd= ocean_struct_rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if(s==null) {
                    return false;
                }else
                {
                    return true;
                }
            }
        });


        return ocean_raw_data_rdd;
    }


}
