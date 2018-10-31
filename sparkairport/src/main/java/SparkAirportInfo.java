import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.*;

public class SparkAirportInfo {
    private static final String COLUMN_OF_ORIGIN_AIRPORT_ID = "_c11";
    private static final String COLUMN_OF_DEST_AIRPORT_ID = "_c14";
    private static final String COLUMN_OF_ARR_DELAY = "_c17";
    private static final String COLUMN_OF_CANCELLED = "_c19";
    private static final String COLUMN_FROM = "FROM";
    private static final String COLUMN_TO = "TO";

    private static final String ORIGIN_AIRPORT_ID = "ORIGIN_AIRPORT_ID";
    private static final String DEST_AIRPORT_ID = "DEST_AIRPORT_ID";
    private static final String DELAY = "DELAY";
    private static final String CANCELLED = "CANCELLED";

    private static final String COLUMN_OF_CODE = "_c0";
    private static final String COLUMN_OF_DESCRIPTION = "_c1";

    private static final String CODE = "CODE";
    private static final String DESCRIPTION = "DESCRIPTION";

    private static final String TOTAL_NUMBER_OF_FLIGHTS = "TOTAL_NUMBER_OF_FLIGHTS";
    private static final String MAX_DELAY = "MAX_DELAY";
    private static final String NUMBER_OF_DELAYED_FLIGHTS = "NUMBER_OF_DELAYED_FLIGHTS";
    private static final String NUMBER_OF_CANCELLED_FLIGHTS = "NUMBER_OF_CANCELLED_FLIGHTS";

    private static final String LEFTOUTER_JOIN_TYPE = "leftouter";
    private static final String FULL_JOIN_TYPE = "full";

    private static final String PERCENT_OF_DELAY = "PERCENT_OF_DELAY";
    private static final String PERCENT_OF_CANCELLED = "PERCENT_OF_CANCELLED";

    private static final String CALC_PERCENT = "CalcPercent";
    private static final String FILTER_DELAY = "filterDelay";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkAirportInfo");
        //conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("WARN");


        SQLContext sqlcontext = new SQLContext(sc);


        Dataset<Row> airports = sqlcontext.read().csv("/AIRPORT_ID.csv");
        Dataset<Row> flightDetails = sqlcontext.read().csv("/ONTIME_sample.csv");

        airports = airports
                .withColumnRenamed(COLUMN_OF_CODE, CODE)
                .withColumnRenamed(COLUMN_OF_DESCRIPTION, DESCRIPTION)
                .where(column(CODE).notEqual("Code"));

        final Broadcast<Dataset> airportsBroadcasted = sc.broadcast(airports);

        /*
        System.out.println((char)27 + "[92m");
        for(AirportDescription line:test.collect()){
            System.out.println("* " + line.getCode() + "  ||  " + line.getDescription());
        }
        System.out.println((char)27 + "[39m");
        */



       flightDetails = flightDetails.select(COLUMN_OF_ORIGIN_AIRPORT_ID, COLUMN_OF_DEST_AIRPORT_ID, COLUMN_OF_ARR_DELAY, COLUMN_OF_CANCELLED)
                .withColumnRenamed(COLUMN_OF_ORIGIN_AIRPORT_ID, ORIGIN_AIRPORT_ID)
                .withColumnRenamed(COLUMN_OF_DEST_AIRPORT_ID, DEST_AIRPORT_ID)
                .withColumnRenamed(COLUMN_OF_ARR_DELAY, DELAY)
                .withColumnRenamed(COLUMN_OF_CANCELLED, CANCELLED)
                //where(col("DELAY").notEqual("null"))
                .filter(row -> !row.getAs(0).toString().equals(ORIGIN_AIRPORT_ID));
                //.where(col("ORIGIN_AIRPORT_ID").notEqual("ORIGIN_AIRPORT_ID"));

        //flightDetails.show();

        flightDetails = flightDetails.select(
                ORIGIN_AIRPORT_ID,
                DEST_AIRPORT_ID,
                DELAY,
                CANCELLED)
                .withColumn(DELAY, flightDetails.col(DELAY).cast(DataTypes.DoubleType))
                .persist(StorageLevel.MEMORY_AND_DISK());

        /*
        flightDetails.printSchema();
        flightDetails.show();
        flightDetails.printSchema();
*/

        sqlcontext.udf().register(FILTER_DELAY, new FilterDelay(), DataTypes.DoubleType);

        Dataset maxDelay = flightDetails
                //.where("DELAY > 0")
                .groupBy(col(ORIGIN_AIRPORT_ID), col(DEST_AIRPORT_ID))
                .agg(functions.max(callUDF(FILTER_DELAY, col(DELAY))).as(MAX_DELAY));


        /*
        flightDetails.where("ORIGIN_AIRPORT_ID == 12478").where("DEST_AIRPORT_ID == 12892").show();
        flightDetails.where("ORIGIN_AIRPORT_ID == 13930").where("DEST_AIRPORT_ID == 14100").show();
        maxDelay.where("ORIGIN_AIRPORT_ID == 12478").where("DEST_AIRPORT_ID == 12892").show();
        maxDelay.where("ORIGIN_AIRPORT_ID == 13930").where("DEST_AIRPORT_ID == 14100").show();
        */
        //maxDelay.show();

        //12478 --> 12892
        //13930 --> 14100

        Dataset totalNumberOfFlights = flightDetails
                .groupBy(flightDetails.col(ORIGIN_AIRPORT_ID), flightDetails.col(DEST_AIRPORT_ID))
                .agg(count(ORIGIN_AIRPORT_ID).as(TOTAL_NUMBER_OF_FLIGHTS));
                //.orderBy(col(ORIGIN_AIRPORT_ID));

        Dataset numberOfDelayedFlights = flightDetails
                .where("DELAY > 0")
                //.where(col(DELAY).$greater(0))
                .groupBy(flightDetails.col(ORIGIN_AIRPORT_ID), flightDetails.col(DEST_AIRPORT_ID))
                .agg(count(DELAY).as(NUMBER_OF_DELAYED_FLIGHTS));
                //.orderBy(ORIGIN_AIRPORT_ID);

        //numberOfDelayedFlights.show();

        ArrayList<String> joinColList = new ArrayList<>();
        joinColList.add(ORIGIN_AIRPORT_ID);
        joinColList.add(DEST_AIRPORT_ID);


        Dataset DelayedJoinTable = totalNumberOfFlights.join(numberOfDelayedFlights, scala.collection.JavaConversions.asScalaBuffer(joinColList), LEFTOUTER_JOIN_TYPE);


        //DelayedJoinTable.show();
        //DelayedJoinTable.where(col("NUMBER_OF_DELAYED_FLIGHTS").equalTo(2)).show();

        sqlcontext.udf().register(CALC_PERCENT, new CalcPercent(), DataTypes.DoubleType);

        Dataset percentOfDelay = DelayedJoinTable
                .groupBy(col(ORIGIN_AIRPORT_ID), col(DEST_AIRPORT_ID))
                .agg(functions.callUDF(CALC_PERCENT, first(col(NUMBER_OF_DELAYED_FLIGHTS)), first(col(TOTAL_NUMBER_OF_FLIGHTS))).as(PERCENT_OF_DELAY));

        //percentOfDelay.show();


        Dataset numberOfCancelledFlights = flightDetails
                .where("CANCELLED == 1")
                .groupBy(col(ORIGIN_AIRPORT_ID), col(DEST_AIRPORT_ID))
                .agg(count(CANCELLED).as(NUMBER_OF_CANCELLED_FLIGHTS));

        //flightDetails.where("CANCELLED == 1").orderBy(ORIGIN_AIRPORT_ID).show();

        //flightDetails.where(isnull(col(DELAY))).orderBy(ORIGIN_AIRPORT_ID).show();


        Dataset CancelledJoinTable = totalNumberOfFlights.join(numberOfCancelledFlights, scala.collection.JavaConversions.asScalaBuffer(joinColList), LEFTOUTER_JOIN_TYPE);

        Dataset percentOfCancelled = CancelledJoinTable
                .groupBy(col(ORIGIN_AIRPORT_ID), col(DEST_AIRPORT_ID))
                .agg(callUDF(CALC_PERCENT, first(NUMBER_OF_CANCELLED_FLIGHTS), first(col(TOTAL_NUMBER_OF_FLIGHTS))).as(PERCENT_OF_CANCELLED));


        //flightDetails.where("ORIGIN_AIRPORT_ID == 13296").where("DEST_AIRPORT_ID == 12953").show();
        //maxDelay.where("ORIGIN_AIRPORT_ID == 11193").orderBy(DEST_AIRPORT_ID).show();




        Dataset report = maxDelay
                .join(percentOfDelay, scala.collection.JavaConversions.asScalaBuffer(joinColList), FULL_JOIN_TYPE)
                .join(percentOfCancelled, scala.collection.JavaConversions.asScalaBuffer(joinColList), FULL_JOIN_TYPE)
                .join(airportsBroadcasted.getValue(), col(ORIGIN_AIRPORT_ID).equalTo(airportsBroadcasted.getValue().col(CODE)), LEFTOUTER_JOIN_TYPE)
                .drop(CODE)
                .withColumnRenamed(DESCRIPTION, COLUMN_FROM)
                .join(airportsBroadcasted.getValue(), col(DEST_AIRPORT_ID).equalTo(airportsBroadcasted.getValue().col(CODE)), LEFTOUTER_JOIN_TYPE)
                .drop(CODE)
                .withColumnRenamed(DESCRIPTION, COLUMN_TO)
                .drop(ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID)
                .select(COLUMN_FROM, COLUMN_TO, MAX_DELAY, PERCENT_OF_DELAY, PERCENT_OF_CANCELLED);


      //  report.printSchema();

        report.show();
    }
}
