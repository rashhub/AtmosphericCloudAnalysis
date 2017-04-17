package App;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Rash on 04/17/2017
 * Initialize Spark Context and Initialize App.Application
 */

public class Application {

    private JavaSparkContext sparkContext=null;


    public Application(String AppName) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(AppName).set("spark.executor.memory", "4g")
                .set("spark.network.timeout","1000").set("spark.executor.heartbeatInterval","100");

        this.sparkContext = new JavaSparkContext(conf);
    }

    public JavaSparkContext getSparkContext() {
        return this.sparkContext;
    }
}
