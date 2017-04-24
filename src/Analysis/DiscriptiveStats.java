package Analysis;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by Rash on 18-04-2017.
 */
public class DiscriptiveStats implements Serializable {

    public DiscriptiveStats() {

    }

    public void station_count(JavaRDD<String> ship_land_rdd,String file_name) throws IOException
    {
        JavaPairRDD<String,Long> stn_count_rdd= ship_land_rdd.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {

                String[] str = s.split(",");



                return new Tuple2<String, Long>(str[7],Long.valueOf(1));
            }
        });

            System.out.println(stn_count_rdd.count());


        JavaPairRDD<String,Long> agg_stn_entries_rdd =stn_count_rdd.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a1, Long a2) throws Exception {
                return (a1+a2);
            }
        });


        JavaPairRDD<Long,String> sorted_agg_stn_rdd = agg_stn_entries_rdd.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return new Tuple2<Long, String>(stringLongTuple2._2(),stringLongTuple2._1());
            }
        }).sortByKey(false);


        System.setOut(new PrintStream(new FileOutputStream(file_name)));

        List<Tuple2<Long,String>> top_stns= sorted_agg_stn_rdd.collect();
        System.out.println("Station Number,Total Readings");

        for(int stn_i=0;stn_i<top_stns.size();stn_i++)
        {
            System.out.println(top_stns.get(stn_i)._2()+","+top_stns.get(stn_i)._1());
        }


    }

    public void location_agg(JavaRDD<String> ship_land_rdd,String file_name) throws IOException
    {
        JavaPairRDD<String,Long> loc_count_rdd= ship_land_rdd.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {

                String[] str = s.split(",");

                String key= str[5].concat(",").concat(str[6]);

                return new Tuple2<String, Long>(key,Long.valueOf(1));
            }
        });


        JavaPairRDD<String,Long> agg_loc_rdd =loc_count_rdd.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a1, Long a2) throws Exception {
                return (a1+a2);
            }
        });


        JavaPairRDD<Long,String> sorted_agg_loc_rdd = agg_loc_rdd.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return new Tuple2<Long, String>(stringLongTuple2._2(),stringLongTuple2._1());
            }
        }).sortByKey(false);

        System.setOut(new PrintStream(new FileOutputStream(file_name)));

        List<Tuple2<Long,String>> loc_data= sorted_agg_loc_rdd.collect();
        System.out.println("Lat,Long,Total Readings");

        for(int loc_i=0;loc_i<loc_data.size();loc_i++)
        {
            System.out.println(loc_data.get(loc_i)._2()+","+loc_data.get(loc_i)._1());
        }


    }

   public void readings_count(JavaRDD<String> cloud_data, String file_name) throws IOException
   {
       JavaPairRDD<String,Long> aggregate_per_year_rdd =cloud_data.mapToPair(new PairFunction<String, String, Long>() {
           @Override
           public Tuple2<String, Long> call(String s) throws Exception {

               String key=s.split(",")[0];

               return new Tuple2<String, Long>(key,Long.valueOf(1));
           }
       }).reduceByKey(new Function2<Long, Long, Long>() {
           @Override
           public Long call(Long a1, Long a2) throws Exception {
               return (a1+a2);
           }
       });

       List<Tuple2<String,Long>> agg_year_count_list =aggregate_per_year_rdd.collect();

       String out_file=file_name.concat("_").concat("byyear");

       System.setOut(new PrintStream(new FileOutputStream(out_file)));

       System.out.println("Year,Total Readings");

       for(int year_i=0;year_i<agg_year_count_list.size();year_i++)
       {
           System.out.println(agg_year_count_list.get(year_i)._1()+","+agg_year_count_list.get(year_i)._2());
       }



       JavaPairRDD<String,Long> aggregate_per_mon_rdd =cloud_data.mapToPair(new PairFunction<String, String, Long>() {
           @Override
           public Tuple2<String, Long> call(String s) throws Exception {

               String key=s.split(",")[0].concat(s.split(",")[1]);

               return new Tuple2<String, Long>(key,Long.valueOf(1));
           }
       }).reduceByKey(new Function2<Long, Long, Long>() {
           @Override
           public Long call(Long a1, Long a2) throws Exception {
               return (a1+a2);
           }
       });

       List<Tuple2<String,Long>> agg_mon_count_list =aggregate_per_mon_rdd.collect();

       String _file=file_name.concat("_").concat("bymon");

       System.setOut(new PrintStream(new FileOutputStream(_file)));

       System.out.println("YearMonth,Total Readings");

       for(int year_i=0;year_i<agg_mon_count_list.size();year_i++)
       {
           System.out.println(agg_mon_count_list.get(year_i)._1()+","+agg_mon_count_list.get(year_i)._2());
       }

   }

    public void readings_time_of_day(JavaRDD<String> cloud_data,String file_name) throws IOException
    {

        JavaPairRDD<String,Long> aggregate_readings_rdd =cloud_data.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {

                String [] str =s.split(",");

                String key=str[0].concat(str[1]).concat(str[2]).concat(str[3]);

                return new Tuple2<String, Long>(key,Long.valueOf(1));
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a1, Long a2) throws Exception {
                return (a1+a2);
            }
        });

        JavaPairRDD<Long,String> swapped_readings_rdd =aggregate_readings_rdd.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Long> line) throws Exception {
                return new Tuple2<Long, String>(line._2(),line._1());
            }
        }).sortByKey(false);


        List<Tuple2<Long,String>> agg_count_list =swapped_readings_rdd.collect();


        System.setOut(new PrintStream(new FileOutputStream(file_name)));

        System.out.println("YearMonthDayHr,Total Readings");

        for(int year_i=0;year_i<agg_count_list.size();year_i++)
        {
            System.out.println(agg_count_list.get(year_i)._2()+","+agg_count_list.get(year_i)._1());
        }


    }

    public void lat_long_by_year(JavaRDD<String> cloud_data,String file_name) throws IOException
    {
        JavaPairRDD<String,Integer> lat_long_year_rdd=cloud_data.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                String str[] =s.split(",");

                String key = str[0].concat(",").concat(str[5]).concat(",").concat(str[6]); //year,lat,long


                return new Tuple2<String, Integer>(key,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a1, Integer a2) throws Exception {
                return (a1+a2);
            }
        });


        JavaPairRDD<Integer,String> swapped_lat_long_year_rdd = lat_long_year_rdd.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> line) throws Exception {
                return new Tuple2<Integer, String>(line._2(),line._1());
            }
        }).sortByKey(false);


        List<Tuple2<Integer,String>> readings_by_year_latlong =swapped_lat_long_year_rdd.collect();

        System.setOut(new PrintStream(new FileOutputStream(file_name)));

        System.out.println("Year,Lat,Long,Total Readings");

        for(int year_i=0;year_i<readings_by_year_latlong.size();year_i++)
        {
            System.out.println(readings_by_year_latlong.get(year_i)._2()+","+readings_by_year_latlong.get(year_i)._1());
        }



    }


}
