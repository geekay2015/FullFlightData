package org.kadam.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by gangadharkadam on 3/3/16.
 *
 * Project Name: FFD
 */
public class GetEngineTimeSeriesCSVDataJava {
    public static void main( String[] args ) throws Exception {
        if( args.length < 2 )
        {
            System.err.println("Usage: GetEngineInformationCSVDataJava <inputFile> <outputFile>");
            System.exit(1);
        }

        //Define the arguments
        final String inFile = args[0];
        final String outFile = args[1];

        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("GetEngineInformationCSVDataJava");

        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        // sc is an existing JavaSparkContext.
        SQLContext sqlContext = new SQLContext(sc);

        //Load and parse the Engine Information data into Spark DataFrames
        DataFrame engineTimeSeriesDF = sqlContext

                //Define the format
                .read().format("com.databricks.spark.csv")

                //use the first line as header
                .option("header", "true")

                //Automatically infer the data types and schema
                .option("inferSchema", "true")

                //load the file
                .load(inFile);

        //Write the DataFrame data to HDFS location in csv format
        engineTimeSeriesDF
                .write()

                //Define the format
                .format("com.databricks.spark.csv")

                //Set the header option
                .option("header", "true")

                //Save it to HDFS location
                .save(outFile);

        //print the csv DataFrame schema
        System.out.println("Validating the schema...");
        engineTimeSeriesDF.printSchema();

        //Register as a temp table
        engineTimeSeriesDF.registerTempTable("ENGINE_TIME_SERIES");

        //print and validate the the schema
        //engineTimeSeriesDF.printSchema();

        System.out.println("SV WRITE- OFFSET(MAX AND MIN)...");

        sqlContext.sql("SELECT max(Offset) as MAX_OFFSET, min(Offset) as MIN_OFFSET, mean(Offset) as AVG_OFFSET FROM ENGINE_TIME_SERIES");

        //Create a Spark Hive Context Over the spark Context
        System.out.println("Initializing Spark Hive Context..");

        //Create a Spark Hive Context Over the spark Context
        HiveContext hc = new HiveContext(sc);

        //hiveContext.sql("SELECT * FROM ENGINE_TIME_SERIES").registerTempTable("ENGINE_TS")

        //create table using HiveQL
        hc.sql("CREATE TABLE IF NOT EXISTS ENGINE_TS1 (" +
                        "OFFSET DECIMAL," +
                        "LP0 DECIMAL," +
                        "LP1 DECIMAL," +
                        "LP2 DECIMAL," +
                        "LP3 DECIMAL," +
                        "LP4 DECIMAL," +
                        "LP5 INT," +
                        "LP6 INT," +
                        "LP7 INT," +
                        "LP8 INT," +
                        "LP9 INT," +
                        "LP10 INT )" +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
        );

        //Load the data into the table using HiveQL
        hc.sql("LOAD DATA INPATH '/user/hive/warehouse/ffd_java_ts/' INTO table ENGINE_TS1");

        // Queries the hive table HiveQL
        System.out.println("Result of 'SELECT *': ");
        System.out.println(hc.sql("SELECT * FROM ENGINE_TS1 where OFFSET >= 29 "));

        //Stop the context
        sc.stop();

    }
}
