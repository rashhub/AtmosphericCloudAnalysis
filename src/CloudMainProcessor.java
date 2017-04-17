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

        Application land_context =new Application("cloud_data");

        CloudDataLoader loader = new CloudDataLoader(land_file_path,ocean_file_path);

        try
        {
            JavaRDD<String> land_data= loader.load_land_data(land_context);
            JavaRDD<String> ocean_data= loader.load_ocean_data(land_context);

            System.out.println("Land data:"+land_data.count());

            System.out.println("Ocean Data:"+ocean_data.count());

        }catch (Exception e)
        {
            System.out.println("Error in Exception of Data load");
        }

        System.out.println();


    }


}
