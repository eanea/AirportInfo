import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;

public class FilterDelay implements UDF1<Double, Double> {
    @Override
    public Double call(Double delay) throws Exception {

        //try {
            /*
            System.out.println((char) 27 + "[93m");
            System.out.println(delay);
            System.out.println((char) 27 + "[39m");
            */

            if (delay != null && delay <= 0.0) {
                return 0.0;
            } else {
                return delay;
            }
        //}
        //catch (Exception e) {
            //System.out.println(from + " --> " + To);
            //System.out.println(delay.toString() + delay);
            //System.out.println(e.getMessage());
        //}

        //return delay;
    }
}
