import org.apache.spark.sql.api.java.UDF2;

import java.text.DecimalFormat;
import java.util.logging.Logger;

public class CalcPercent implements UDF2<Long, Long, Double> {
    private static final DecimalFormat df = new DecimalFormat("#.###");
    @Override
    public Double call(Long score, Long total) throws Exception {

/*
        System.out.println((char)27 + "[93m");
        System.out.println("total = " + total + " & score = " + score + ";");
        System.out.println((char)27 + "[39m");
        */
        if (score == null) {
            /*
            System.out.println((char)27 + "[93m");
            System.out.println("SHIT! its null");
            System.out.println((char)27 + "[39m");
            */

            return 0.0;
        }
        double percent = (100.0 * score) / total;
/*
        System.out.println((char)27 + "[93m");
        System.out.println("percent = " + percent);
        System.out.println((char)27 + "[39m");
*/
        return Double.parseDouble(df.format(percent));
    }
}
