/**
 * (c) 2016 GE all rights reserved.
 * Author: Gangadhar Kadam
 * Version 1.0:
 *
 * Client: GE Aviation
 *
 * Input: Engine Time Series CSV files
 *
 * Processing desired:
 * Read the csv file into spark, write the files as CSV on hdfs and Load them as HIVE  tables.
 *
 * Framework:
 * 1. Read the CSV file as textFile
 * 2. Split the lines delimited by semicolon
 * 3. Map the splits to SparkSQL Row
 * 4. Define a schema using spark SQL Struct Type
 * 5. Save the CSV to HDFS in CSV format
 * 6. Create a Spark DataFrame using Spark SQL Context
 */

package org.kadam.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;


/**
 * Created by gangadharkadam on 3/3/16.
 * Project Name: FFD
 */

public class GetEngineTimeSeriesCSVDataJava {
    public static void main( String[] args ) throws Exception {

        if( args.length < 1 )
        {
            System.err.println("Usage: GetEngineInformationCSVDataJava <inFile>");
            System.exit(1);
        }

        //Define the arguments
        final String inFile = args[0];
        //final String outFile = args[1];

        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("GetEngineTimeSeriesCSVDataJava");

        // Create a Java version of the Spark Context from the configuration
        System.out.println("Initializing Spark Context..");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Create a Spark Hive Context Over the spark Context
        System.out.println("Initializing Spark Hive Context..");
        HiveContext sqlContext = new HiveContext(sc);

        // sc is an existing JavaSparkContext.
        //System.out.println("Initializing Spark SQL Context..");
        //SQLContext sqlContext = new SQLContext(sc);

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

        //Check some records
        engineTimeSeriesDF.select("Offset").distinct().show();

        //print the schema
        //System.out.println("CSV DataFrame Schema..");
        //engineTimeSeriesDF.printSchema();


        //Write the DataFrame data to HDFS location in csv format
        System.out.println("Writing the DataFrame as parquet File..");
        engineTimeSeriesDF
                .write()
                //Define the format
                .parquet("enginetimeseries.parquet");

        DataFrame parquetFile = sqlContext.read().parquet("enginetimeseries.parquet");

        System.out.println("Registering as a Temporary Table..");
        parquetFile.registerTempTable("enginetimeseries");

        System.out.println("Creating hive table From Temporary Table..");
        sqlContext.sql("CREATE TABLE ETS as " +
                "SELECT * FROM enginetimeseries");


        System.out.println("Querying the Hive Table.....");
        sqlContext.sql("SELECT * FROM ETS where Offset > 28");

        //Stop the context
        System.out.println("Stopping the Spark Context...");
        sc.stop();
    }
}
