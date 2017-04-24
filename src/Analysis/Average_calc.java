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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Rash on 23-04-2017.
 */
public class Average_calc implements Serializable{

    public void average_calculation(JavaRDD<String> cloud_rdd,String metric_name, int metric_id) throws IOException
    {
            final int metric_index=metric_id;

            JavaPairRDD<String,Long> year_count_rdd = cloud_rdd.mapToPair(new PairFunction<String, String, Long>() {
                @Override
                public Tuple2<String, Long> call(String s) throws Exception {

                    String key=s.split(",")[0];
                    String value=s.split(",")[metric_index];

                    return new Tuple2<String, Long>(key,Long.valueOf(value));
                }
            });

            JavaPairRDD<String,Long> year_sum_by_key = year_count_rdd.reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long a1, Long a2) throws Exception {

                    return (a1+a2);
                }
            });


            JavaPairRDD<Long,String> swapped_sum_by_key= year_sum_by_key.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
                @Override
                public Tuple2<Long, String> call(Tuple2<String, Long> line) throws Exception {
                    return new Tuple2<Long, String>(line._2(),line._1());
                }
            }).sortByKey(false);

            List<Tuple2<Long,String>> year_sum =swapped_sum_by_key.collect();
            Map<String,Object> year_count_by_key = year_count_rdd.countByKey();

            String year_file_name= "year".concat("_").concat(metric_name);
            System.setOut(new PrintStream(new FileOutputStream(year_file_name)));
            System.out.println("year,"+metric_name+" Average");

            for(int year_i=0;year_i<year_sum.size();year_i++)
            {
                String key = year_sum.get(year_i)._2();

                float numerator = year_sum.get(year_i)._1();

                float denom = Long.valueOf(year_count_by_key.get(key).toString());

                float year_average= numerator/denom;

                System.out.println(key+","+year_average);

            }


            JavaPairRDD<String,Long> mon_count_rdd = cloud_rdd.mapToPair(new PairFunction<String, String, Long>() {
                @Override
                public Tuple2<String, Long> call(String s) throws Exception {

                    String key= s.split(",")[0].concat(s.split(",")[1]);
                    String value = s.split(",")[metric_index];

                    return new Tuple2<String, Long>(key,Long.valueOf(value));
                }
            });



            JavaPairRDD<String,Long> mon_sum_by_key = mon_count_rdd.reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long a1, Long a2) throws Exception {

                    return (a1+a2);
                }
            });

            JavaPairRDD<Long,String> swapped_mon_sum_by_key=mon_sum_by_key.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
                @Override
                public Tuple2<Long, String> call(Tuple2<String, Long> line) throws Exception {

                    return new Tuple2<Long, String>(line._2(),line._1());
                }
            }).sortByKey(false);


            List<Tuple2<Long,String>> mon_sum =swapped_mon_sum_by_key.collect();
            Map<String,Object> mon_count_by_key = mon_count_rdd.countByKey();

            String mon_file_name= "month".concat("_").concat(metric_name);
            System.setOut(new PrintStream(new FileOutputStream(mon_file_name)));
            System.out.println("Year,Month,"+metric_name+" Average");

            for(int mon_i=0;mon_i<mon_sum.size();mon_i++)
            {
                String key = mon_sum.get(mon_i)._2();

                float numerator = mon_sum.get(mon_i)._1();

                float denom = Long.valueOf(mon_count_by_key.get(key).toString());

                float mon_average= numerator/denom;

                System.out.println(key+","+mon_average);

            }

            JavaPairRDD<String,Long> day_count_rdd = cloud_rdd.mapToPair(new PairFunction<String, String, Long>() {
                @Override
                public Tuple2<String, Long> call(String s) throws Exception {

                    String key = s.split(",")[0].concat(s.split(",")[1]).concat(s.split(",")[2]);
                    String value=s.split(",")[metric_index];

                    return new Tuple2<String, Long>(key,Long.valueOf(value));
                }
            });


             JavaPairRDD<String,Long>  day_sum_by_key = day_count_rdd.reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long a1, Long a2) throws Exception {

                    return (a1+a2);
                }
            });

            JavaPairRDD<Long,String> swapped_day_sum =day_sum_by_key.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
                @Override
                public Tuple2<Long, String> call(Tuple2<String, Long> line) throws Exception {
                    return new Tuple2<Long, String>(line._2(),line._1());
                }
            });


            List<Tuple2<Long,String>> day_sum = swapped_day_sum.collect();

            Map<String,Object> day_count_by_key = day_count_rdd.countByKey();

            String day_file_name= "day".concat("_").concat(metric_name);
            System.setOut(new PrintStream(new FileOutputStream(day_file_name)));
            System.out.println("Year,Month,Day,"+metric_name+" Average");

            for(int day_i=0;day_i<day_sum.size();day_i++)
            {
                String key = day_sum.get(day_i)._2();

                float numerator = day_sum.get(day_i)._1();

                float denom = Long.valueOf(day_count_by_key.get(key).toString());

                float day_average= numerator/denom;

                System.out.println(key+","+day_average);

            }


    }

    public void obscured_sky(JavaRDD<String> cloud_data,String file_name) throws IOException
    {
        JavaPairRDD<String,Integer> obs_cloud_count_rdd = cloud_data.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                String str[] = s.split(",");
                String key =str[0].concat(",").concat(str[1]);
                int value=0;

                if(str[10].equals('8') && (str[15].equals("10")||str[15].equals("11")||str[16].equals("10")))
                {
                    value = 1;
                }

                return new Tuple2<String, Integer>(key,value);
            }
        });

        Map<String,Object> obs_count = obs_cloud_count_rdd.countByKey();

        List<Tuple2<String,Integer>> obs_lines_sum=   obs_cloud_count_rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a1, Integer a2) throws Exception {
                return (a1+a2);
            }
        }).collect();


        System.setOut(new PrintStream(new FileOutputStream(file_name)));
        System.out.println("Year,Month"+",Average");

        for(int key_i=0;key_i<obs_lines_sum.size();key_i++)
        {
            String key = obs_lines_sum.get(key_i)._1();

            float numerator = obs_lines_sum.get(key_i)._2();

            float denom = Long.valueOf(obs_count.get(key).toString());

            float average= numerator/denom;

            System.out.println(key+","+average);

        }


    }




}
