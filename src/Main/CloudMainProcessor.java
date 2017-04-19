package Main;

import Analysis.DiscriptiveStats;
import App.Application;
import PreProcessing.CloudDataLoader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;

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

        Application land_context =new Application("cloud_data");

        CloudDataLoader loader = new CloudDataLoader(land_file_path,ocean_file_path);

        try
        {
            JavaRDD<String> land_data= loader.load_land_data(land_context);
            JavaRDD<String> ocean_data= loader.load_ocean_data(land_context);

            System.out.println("Land data:"+land_data.count());

            System.out.println("Ocean Data:"+ocean_data.count());

            DiscriptiveStats ds =new DiscriptiveStats();

            JavaPairRDD<String,Long> land_stn_stats= ds.station_count(land_data);
            JavaPairRDD<String,Long> ocean_stn_stats= ds.station_count(ocean_data);

            JavaPairRDD<Long,String> sorted_land_stn_stats =land_stn_stats.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
                @Override
                public Tuple2<Long, String> call(Tuple2<String, Long> inputrecord) throws Exception {
                    return new Tuple2<Long, String>(inputrecord._2(),inputrecord._1());
                }
            }).sortByKey(false);


            JavaPairRDD<Long,String> sorted_ocean_stn_stats =ocean_stn_stats.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
                @Override
                public Tuple2<Long, String> call(Tuple2<String, Long> inputrecord) throws Exception {
                    return new Tuple2<Long, String>(inputrecord._2(),inputrecord._1());
                }
            }).sortByKey(false);


            System.setOut(new PrintStream(new FileOutputStream("topStns")));

            List<Tuple2<Long,String>> top_land_stns= sorted_land_stn_stats.take(20);

            System.out.println("Station Number,Total Readings");

            for(int land_i=0;land_i<top_land_stns.size();land_i++)
            {
                System.out.println(top_land_stns.get(land_i)._2()+","+top_land_stns.get(land_i)._1());
            }


        }catch (Exception e)
        {
            System.out.println("Error in Exception of Data load"+e);
        }

        System.out.println();


    }


}
