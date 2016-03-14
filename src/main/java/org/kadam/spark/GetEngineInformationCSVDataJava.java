/**
 * (c) 2016 GE all rights reserved.
 * Author: Gangadhar Kadam
 * Version 1.0:
 *
 * Client: GE Aviation
 *
 * Input: Full flight Engine Information CSV files
 *
 * Processing desired:
 * Read the csv file into spark, write the files as CSV on hdfs and Load them as HIVE & HAWQ tables.
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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.*;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by gangadharkadam on 3/3/16.
 * Project Name: FFD
 */

public class GetEngineInformationCSVDataJava {
    public static void main( String[] args ) throws Exception
    {
        if( args.length < 2 )
        {
            System.err.println("Usage: GetEngineInformationCSVDataJava <inputFile> <outputFile>");
            System.exit(1);
        }

        //Define the arguments
        final String inputFile = args[0];
        final String outputFile = args[1];

        // final String inputFile  = "/Users/gangadharkadam/myapps/FullFlightData/src/main/resources/engineInformation.csv";
        // final String outputFile = "hdfs://localhost:9000/user/hive/warehouse/ffd_java_data";


        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("GetEngineInformationCSVDataJava");

        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Define a SQL Context
        SQLContext sqlContext = new SQLContext(sc);


        // Load a CSV file and convert each line to a JavaBean
        JavaRDD<String> engineRDD = sc.textFile(inputFile);
        for (String s : engineRDD.collect()) {
             System.out.println();
        }


        //use flatMap to flatten the result
        //JavaRDD<String> engineKV = engineRDD.flatMap(
         //       new FlatMapFunction<String, String>() {
          //          public Iterable<String> call(String s) {
           //             return Arrays.asList(s.split(";"));
           //         }
            //    });

        // The schema is encoded in a string
        String schemaString =
                "CUSTOMER_CODE TAIL_NUMBER FLIGHT_RECORD_NUMBER EXPORT_CONFIG_VERSION FLIGHT_NUMBER " +
                "FLIGHT_DATE_EXACT DECODE_GENERATION_TIME AIRPORT_DEPART AIRPORT_ARRIVAL FILE_TYPE";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName: schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);

        //Then create the person, using the address struct
        /*
        List<StructField> engineInfoFields = new ArrayList<StructField>();
        fields.add(DataType.createStructField("CUSTOMER_CODE", DataType.StringType, true));
        fields.add(DataType.createStructField("TAIL_NUMBER", DataType.IntType, true));
        fields.add(DataType.createStructField("FLIGHT_RECORD_NUMBER", addressStruct, true));
        fields.add(DataType.createStructField("EXPORT_CONFIG_VERSION", DataType.StringType, true));
        fields.add(DataType.createStructField("FLIGHT_NUMBER", DataType.IntType, true));
        fields.add(DataType.createStructField("FLIGHT_DATE_EXACT", addressStruct, true));
        fields.add(DataType.createStructField("DECODE_GENERATION_TIME", DataType.StringType, true));
        fields.add(DataType.createStructField("AIRPORT_DEPART", DataType.IntType, true));
        fields.add(DataType.createStructField("FILE_TYPE", addressStruct, true));
        */


        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> engineRowRDD = engineRDD.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        String[] fields = record.split(";");
                        return RowFactory.create(
                                fields[0].split("=",2)[1],
                                fields[1].split("=",2)[1],
                                fields[2].split("=",2)[1],
                                fields[3].split("=",2)[1],
                                fields[4].split("=",2)[1],
                                fields[5].split("=",2)[1],
                                fields[6].split("=",2)[1],
                                fields[7].split("=",2)[1],
                                fields[8].split("=",2)[1],
                                fields[9].split("=",2)[1]
                        );

                    }
                });

        // Apply a schema to an RDD of Java Beans
        DataFrame engineDF = sqlContext.createDataFrame(engineRowRDD, schema);

        // Take a look at the schema of this new DataFrame.
        engineDF.printSchema();

        // Register this DataFrame as a table.
        engineDF.registerTempTable("egineInformation");

        //save the file as csv
        engineDF.write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save(outputFile);

        //Print the data frame
        engineDF.show();


        //Create a Spark Hive Context Over the spark Context
        HiveContext hc = new HiveContext(sc);

        //create table using HiveQL
        hc.sql("CREATE TABLE IF NOT EXISTS ENGINE_INFO (\n" +
                "CUSTOMER_CODE STRING,\n" +
                "TAIL_NUMBER STRING,\n" +
                "FLIGHT_RECORD_NUMBER INT,\n" +
                "EXPORT_CONFIG_VERSION INT,\n" +
                "FLIGHT_NUMBER STRING,\n" +
                "FLIGHT_DATE_EXACT STRING,\n" +
                "DECODE_GENERATION_TIME STRING,\n" +
                "AIRPORT_DEPART STRING,\n" +
                "AIRPORT_ARRIVAL STRING,\n" +
                "FILE_TYPE STRING\n" +
                ")\n" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
        );

        //Load the data into the table using HIVEQL
        hc.sql("LOAD DATA INPATH '/user/hive/warehouse/ffd_java_data' INTO table ENGINE_INFO");

        // Query the hive table HiveQL
        for (Row row : hc.sql("SELECT * FROM ENGINE_INFO").collect()) {
            System.out.println();
        }

        //Stop the context
        sc.stop();
    }
}

