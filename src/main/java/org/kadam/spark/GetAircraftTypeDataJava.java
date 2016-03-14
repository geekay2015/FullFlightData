/**
 * (c) 2016 GE all rights reserved.
 * Author: Gangadhar Kadam
 * Version 1.0:
 *
 * Client: GE Aviation
 *
 * Input: Aircraft Type CSV files
 *
 * Processing desired:
 * Read the csv file into spark, write the files as CSV on hdfs and Load them as HIVE tables.
 *
 * Framework:
 * 1. Read the CSV file as textFile
 * 2. Split the lines delimited by pipe
 * 3. Map the splits to SparkSQL Row
 * 4. Define a schema using spark SQL Struct Type
 * 5. Save the dataFrame as a Hive Table
 * 6. Create a Spark DataFrame using Spark SQL Context
 */

package org.kadam.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/**
 * Created by gangadharkadam on 3/11/16.
 * Project Name: FullFlightData}
 */

public class GetAircraftTypeDataJava {


    public static void main(String args[]) throws Exception{

        if (args.length < 1 ) {
            System.out.println("Usage: GetAircraftTypeDataJava <InputFile> <outputFile>");
            System.exit(1);

        }

        // Define the argument
        //String inputFile = "file:///Users/gangadharkadam/myapps/FullFlightData/src/main/resources/PhmAircraftType.csv";
        String inputFile = args[0];

        // Define the Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("GetAircraftTypeDataJava")
                .setMaster("local")
                .set("spark.speculation", "false");

        // Define the Java Spark Context
        System.out.println("Initializing Spark Context..");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);



        // Set the Hive Context
        System.out.println("Initializing Spark Hive Context..");
        HiveContext sqlContext = new HiveContext(sparkContext);

        // Read and parser the Aircraft type CSV file to a JavaRDD
        JavaRDD<String> inputRDD = sparkContext.textFile(inputFile,0);

        inputRDD.take(10).forEach(System.out::println);

        // Define a Schema String
        // Generate the schema based on the string of schema
        //StructType airFrameSchema = DataTypes.createStructType(fields);

        StructType airFrameSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("AIRCRFT_TYP_CD", DataTypes.StringType, true),
                DataTypes.createStructField("NO_OF_ENGS", DataTypes.IntegerType, true),
                DataTypes.createStructField("VLD_ARFRMR_NM", DataTypes.StringType, true),
                DataTypes.createStructField("SRC_ARFRMR_NM", DataTypes.StringType, true),
                DataTypes.createStructField("ARFRMR_VLDTN_DT", DataTypes.StringType, true),
                DataTypes.createStructField("LGCL_DEL_IND", DataTypes.StringType, true),
                DataTypes.createStructField("JOB_CONTROL_ID", DataTypes.IntegerType, true)
        });

        //Convert records of the inputRDD to Rows

        JavaRDD<Row> inputRowRDD = inputRDD.map(
                new Function<String, Row>() {
                    static final long serialVersionUID = 1L;
                    public Row call(String record) throws Exception {
                        String[] columns = record.split("\\|");
                        return  RowFactory.create(
                                columns[0],
                                Integer.parseInt(columns[1]),
                                columns[2],
                                columns[3],
                                columns[4],
                                columns[5],
                                Integer.parseInt(columns[6])
                                );
                    }
                });

        // Apply a schema to the Java ROW RDD
        DataFrame aircraftTypeDF = sqlContext.createDataFrame(inputRowRDD, airFrameSchema);

        //Check few records from the Data Frame
        aircraftTypeDF.printSchema();

        Row[] take = aircraftTypeDF.take(10);
        for (Row rows : take) {
            System.out.println(rows);
        }

        // Save the DataFrame as a table
        aircraftTypeDF.saveAsTable("aircrafttype", "parquet", SaveMode.Overwrite);

        // Query the saved table
        sqlContext.sql("show tables").show();
        sqlContext.sql("describe aircrafttype").show();
        sqlContext.sql("SELECT * FROM aircrafttype where VLD_ARFRMR_NM = 'AIRBUS'").show();

        //Stop the context
        System.out.println("Stopping the Spark Context...");
        sparkContext.stop();

    }
}
