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
// scalastyle:off println

package org.kadam.spark

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StructField, StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}



/**
 * Created by 502552987 on 2/24/16.
 * gangadhar.kadam@ge.com
 * Project Name: FFD
 */

object GetEngineInformationCSVDataToHDFS {

  //Define the Application Name
  val AppName: String ="EngineInformationCSVDataLoad"

  //set the logging levels
  Logger.getLogger("org.apache").setLevel(Level.ERROR)


  //Main Method
  def main(args: Array[String]): Unit = {

    //Check the arguments

    if (args.length < 2) {
      System.err.println("Usage: " + this.getClass.getSimpleName + " <inputFile>  <outputFile>")
      System.exit(1)
    }


    //Define the arguments
    val inputFile = args(0)
    val outputFile = args(1)

    //val inputFile: String = "/Users/gangadharkadam/myapps/FullFlightData/src/main/resources/engineInformation.csv"
    //val outputFile: String = "hdfs://localhost:9000/user/hive/warehouse/ffd_data"


    // Create the Spark configuration and the spark context
    println("Initializing the Spark Context...")
    val conf = new SparkConf()
      .setAppName(AppName)
      .setMaster("local")

    //Define the Spark Context
    val sc = new SparkContext(conf)

    //Define the SQL Context
    val sqlContext = new SQLContext(sc)

    //Load and parse the Engine Information data into Spark DataFrames
    val engineRDD = sc.textFile(inputFile)

      //Split the Delimited lines to get the column values
      .map(line => line.split(";"))

      //map each element to Spark SQL Row and get the value
      .map(p => Row(
        p(0).split("=")(1),
        p(1).split("=")(1),
        p(2).split("=")(1).toInt,
        p(3).split("=")(1).toInt,
        p(4).split("=")(1),
        p(5).split("=")(1),
        p(6).split("=")(1),
        p(7).split("=")(1),
        p(8).split("=")(1),
        p(9).split("=")(1)
      )
      )

    //Call a collect action to validate the csv load
      engineRDD.collect().foreach(println)

    //Define the Schema using Spark Struct Fields
    val schema = StructType(Array
    (
      StructField("CUSTOMER_CODE",StringType,true),
      StructField("TAIL_NUMBER",StringType,true),
      StructField("FLIGHT_RECORD_NUMBER",IntegerType,true),
      StructField("EXPORT_CONFIG_VERSION",IntegerType,true),
      StructField("FLIGHT_NUMBER",StringType,true),
      StructField("FLIGHT_DATE_EXACT",StringType,true),
      StructField("DECODE_GENERATION_TIME",StringType,true),
      StructField("AIRPORT_DEPART",StringType,true),
      StructField("AIRPORT_ARRIVAL",StringType,true),
      StructField("FILE_TYPE",StringType,true)
    )
    )

    //Create a DataFrame using the input RDD and the schems
    val engineSchemaRDD = sqlContext.createDataFrame(engineRDD, schema)

    //save the file as csv
    engineSchemaRDD
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(outputFile)

    //Print the data frame
    engineSchemaRDD.show()

    //print and validate the the schema
    engineSchemaRDD.printSchema()

    //Create a Spark Hive Context Over the spark Context
    println("Initializing Spark Hive Context..")
    val hiveContext = new HiveContext(sc)
    import hiveContext.sql

    //create table using HiveQL
    hiveContext.sql(
      """
        |CREATE TABLE IF NOT EXISTS ENGINE_INFORMATION (
        |CUSTOMER_CODE STRING,
        |TAIL_NUMBER STRING,
        |FLIGHT_RECORD_NUMBER INT,
        |EXPORT_CONFIG_VERSION INT,
        |FLIGHT_NUMBER STRING,
        |FLIGHT_DATE_EXACT STRING,
        |DECODE_GENERATION_TIME STRING,
        |AIRPORT_DEPART STRING,
        |AIRPORT_ARRIVAL STRING,
        |FILE_TYPE STRING
        |)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
      """.stripMargin)

    //Load the data into the table using HIVEQL
    hiveContext.sql(
      s"""
        |LOAD DATA INPATH \"${outputFile}\" INTO table ENGINE_INFORMATION
      """.stripMargin)

    // Queries the hive table HiveQL
    println("Result of 'SELECT *': ")
    sql("SELECT * FROM ENGINE_INFORMATION").collect().foreach(println)

    //stop the context
    println("Stopping Spark Context..")
    sc.stop()

  }
}
// scalastyle:on println