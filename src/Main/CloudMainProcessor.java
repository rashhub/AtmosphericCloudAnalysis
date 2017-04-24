package Main;

import Analysis.Average_calc;
import Analysis.DiscriptiveStats;
import App.Application;
import PreProcessing.CloudDataLoader;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by Rash on 17-04-2017.
 */
public class CloudMainProcessor {

    public static void main(String args[])
    {
        if (args.length < 1) {
            System.err.println("Usage: Data Pre-Processing <file>");
            System.exit(1);
        }

        String land_file_path=args[0];
        String ocean_file_path=args[1];

        System.out.println("Paths :"+land_file_path+":"+ocean_file_path);

        Application land_context =new Application("atmospheric_cloud_analysis");

        CloudDataLoader loader = new CloudDataLoader(land_file_path,ocean_file_path);


        try
        {
            JavaRDD<String> land_data= loader.load_land_data(land_context);

            JavaRDD<String> ocean_data= loader.load_ocean_data(land_context);

            System.out.println("Land data:"+land_data.count());

            System.out.println("Ocean Data:"+ocean_data.count());


          //  land_data.cache();
            //ocean_data.cache();

            DiscriptiveStats ds =new DiscriptiveStats();

            //Get Land/Ocean top station information
           /* ds.station_count(land_data,"topLandStns");
            ds.station_count(ocean_data,"topOceanStns");
            ds.location_agg(land_data,"landLocStats");
            ds.location_agg(ocean_data,"oceanLocStats");*/

            //Reading Counts for land and ocean per year
            ds.readings_count(land_data,"land_agg_readings");
            ds.readings_count(ocean_data,"ocean_agg_readings");
            ds.readings_time_of_day(land_data,"land_time_data");
            ds.readings_time_of_day(ocean_data,"ocean_time_data");
            ds.lat_long_by_year(land_data,"land_year_by_lat_long");
            ds.lat_long_by_year(ocean_data,"ocean_year_by_lat_long");


             Average_calc calc=new Average_calc();

            //Land Calculations -- Get averages per year for all the matrics
            calc.average_calculation(land_data,"land_air_temp",24);
            calc.average_calculation(land_data,"land_wind_speed",22);
            calc.average_calculation(land_data,"land_brightness",4);
            calc.average_calculation(land_data,"land_elevation",26);
            calc.average_calculation(land_data,"land_total_cover",10);
            calc.average_calculation(land_data,"land_lower_cloud",12);
            calc.average_calculation(land_data,"land_mid_cover",13);
            calc.average_calculation(land_data,"land_high_cover",14);
            calc.average_calculation(land_data,"land_lower_cloud_base_ht",11);
            calc.average_calculation(land_data,"land_sea_pressure",21);
            calc.average_calculation(land_data,"land_dew_point",25);
            calc.obscured_sky(land_data,"Obscured_land_data");

            //Ship/Ocean Calculation --Get averages for all the matrics
            calc.average_calculation(ocean_data,"ocean_air_temp",24);
            calc.average_calculation(ocean_data,"ocean_wind_speed",22);
            calc.average_calculation(ocean_data,"ocean_brightness",4);
            calc.average_calculation(ocean_data,"ocean_sea_surf_temp",26);
            calc.average_calculation(ocean_data,"ocean_total_cover",10);
            calc.average_calculation(ocean_data,"ocean_lower_cloud",12);
            calc.average_calculation(ocean_data,"ocean_mid_cover",13);
            calc.average_calculation(ocean_data,"ocean_high_cover",14);
            calc.average_calculation(ocean_data,"ocean_lower_cloud_base_ht",11);
            calc.average_calculation(ocean_data,"ocean_sea_pressure",21);
            calc.average_calculation(ocean_data,"ocean_dew_point",25);
            calc.obscured_sky(ocean_data,"Obscured_ocean_data");


        }catch (Exception e)
        {
            System.out.println("Error in Exception of Data load"+e);
        }


    }


}
