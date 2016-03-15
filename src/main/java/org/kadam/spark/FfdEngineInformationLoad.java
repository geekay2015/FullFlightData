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
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by gangadharkadam on 3/14/16.
 * Project Name: FFD
 */


public class FfdEngineInformationLoad {
    public static void main( String[] args ) throws Exception
    {
        if (args.length < 1 ) {
            System.out.println("Usage: FfdEngineInformationLoad <InputFile>");
            System.exit(1);

        }

        // Define the argument
        //String inputFile = "file:///Users/gangadharkadam/myapps/FullFlightData/src/main/resources/PhmAircraftType.csv";
        String inputFile = args[0];

        // Define the Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("FfdEngineInformationLoad")
                .setMaster("local[*]")
                .set("spark.speculation", "false");

        // Define the Java Spark Context
        System.out.println("Initializing Spark Context..");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);


        // Set the Hive Context
        System.out.println("Initializing Spark Hive Context..");
        HiveContext sqlContext = new HiveContext(sparkContext);

        // Read and parser the Aircraft type CSV file to a JavaRDD

        JavaRDD<String> engineRDD = sparkContext.textFile(inputFile,0);

        engineRDD.take(10).forEach(System.out::println);

        // Define a Schema String
        // Generate the schema based on the string of schema
        //StructType airFrameSchema = DataTypes.createStructType(fields);

        // The schema is encoded in a string
        String schemaString =
                "FFD_ID AIRCRAFT_TYPE CUSTOMER_ICAO_CODE TAIL_NUMBER ADI_FLIGHT_RECORD_NUMBER EXPORT_CONFIG_VERSION FLIGHT_NUMBER " +
                        "FLIGHT_DATE_EXACT DECODE_GENERATION_DATE AIRPORT_DEPART AIRPORT_DESTINATION ENGINE_POSITION FILE_TYPE";


        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName: schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType engineInformationSchema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> engineRowRDD = engineRDD.map(
                record -> {
                    String[] columns = record.split(";");
                    return RowFactory.create(
                            columns[0].split("=", 2)[1],
                            columns[1].split("=", 2)[1],
                            columns[2].split("=", 2)[1],
                            columns[3].split("=", 2)[1],
                            columns[4].split("=", 2)[1],
                            columns[5].split("=", 2)[1],
                            columns[6].split("=", 2)[1],
                            columns[7].split("=", 2)[1],
                            columns[8].split("=", 2)[1],
                            columns[9].split("=", 2)[1],
                            columns[10].split("=", 2)[1],
                            columns[11].split("=", 2)[1],
                            columns[12].split("=", 2)[1]
                    );

                });
        // Apply a schema to the Java ROW RDD
        DataFrame engineDF = sqlContext.createDataFrame(engineRowRDD, engineInformationSchema);

        // Take a look at the schema of this new DataFrame.
        engineDF.printSchema();

        Row[] take = engineDF.take(10);
        for (Row rows : take) {
            System.out.println(rows);
        }

        // Save the DataFrame as a table
        engineDF.saveAsTable("engine_information", "parquet", SaveMode.Overwrite);


        // Query the saved table
        sqlContext.sql("show tables").show();
        sqlContext.sql("describe engine_information").show();

        sqlContext.sql("SELECT * FROM engine_information where customer_icao_code = 'GLO'").show();

    }
}

