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
 * Read the FFD Engine Time Series csv file into spark, write the files as CSV on hdfs and Load them as HIVE & HAWQ tables.
 *
 * Framework:
 * 1. Read the CSV file using databricks csv package
 * 2. Automatically Infer the Schema and load the columns
 * 3. Save the CSV to HDFS in CSV format
 * 4. Create a Spark DataFrame using Spark SQL Context
 * 5. Load the file to spark
 */
// scalastyle:off println

package org.kadam.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by 502552987 on 2/24/16.
 * gangadhar.kadam@ge.com
 * Project Name: FFD
 */

object GetEngineTimeSeriesCSVDataToHDFS {

  //Define the Application Name
  val AppName: String ="EngineTimeSeriesCSVDataToHDFS"

  //set the logging levels
  Logger.getLogger("org.apache").setLevel(Level.ERROR)


  //Main Method
  def main(args: Array[String]): Unit = {
    //Check the arguments

    if (args.length < 2 ) {
      System.err.println("Usage: " + this.getClass.getSimpleName + " <inputFile>  <outputFile>")
      System.exit(1)
    }


    //Define the arguments
    val inFile = args(0)
    val outFile = args(1)
    //val orcFile = args(2)

    //Create the Spark configuration and the spark context
    println("Initializing the Spark Context...")
    val conf = new SparkConf()
      .setAppName(AppName)
      .setMaster("local")

    //Define the Spark Context
    val sc = new SparkContext(conf)

    //Define the SQL Context
    val sqlContext = new SQLContext(sc)

    //Load and parse the Engine Information data into Spark DataFrames
    val engineTimeSeriesDF = sqlContext

      //Define the format
      .read.format("com.databricks.spark.csv")

      //use the first line as header
      .option("header", "true")

      //Automatically infer the data types and schema
      .option("inferSchema", "true")

      //load the file
      .load(inFile)

    //Print the schema
    //println("Engine Time Series Schema - FROM CSV...")
    //engineTimeSeriesDF.printSchema()

    //Write the DataFrame data to HDFS location in csv format
    engineTimeSeriesDF
      .write

      //Define the format
      .format("com.databricks.spark.csv")

      //Set the header option
      .option("header", "true")

      //Save it to HDFS location
      .save(outFile)

    //print the csv DataFrame schema
    println("Validating the schema...")
    engineTimeSeriesDF.printSchema()


    //Register as a temp table
    engineTimeSeriesDF.registerTempTable("ENGINE_TIME_SERIES")

    println("CSV WRITE- OFFSET(MAX AND MIN)...")
    sqlContext.sql(
      """
        |SELECT
        |max(Offset) as MAX_OFFSET,
        |min(Offset) as MIN_OFFSET,
        |mean(Offset) as AVG_OFFSET,
        |stddev(Offset) as STD_OFFSET,
        |count(Offset) as CNT_OFFSET
        |FROM ENGINE_TIME_SERIES
      """.stripMargin).collect.foreach(println)

    //Save the temp table as ORC format
    //engineTimeSeriesDF
     // .write
     // .format("parquet")
     // .mode("append")
     // .saveAsTable("ENGINE_TS")

    //Create a Spark Hive Context Over the spark Context
    println("Initializing Spark Hive Context..")
    val hiveContext = new HiveContext(sc)
    import hiveContext.sql

    //hiveContext.sql("SELECT * FROM ENGINE_TIME_SERIES").registerTempTable("ENGINE_TS")


    //create table using HiveQL
    hiveContext.sql(
      """
        |CREATE TABLE IF NOT EXISTS ENGINE_TS (
        | OFFSET DECIMAL,
        | LP0 DECIMAL,
        | LP1 DECIMAL,
        | LP2 DECIMAL,
        | LP3 DECIMAL,
        | LP4 DECIMAL,
        | LP5 INT,
        | LP6 INT,
        | LP7 INT,
        | LP8 INT,
        | LP9 INT,
        | LP10 INT
        |)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
      """.stripMargin)

    //Load the data into the table using HIVEQL
    hiveContext.sql(
      s"""
         |LOAD DATA INPATH \"${outFile}\" INTO table ENGINE_TS
      """.stripMargin)


    // Queries the hive table HiveQL
    println("Result of 'SELECT *': ")
    sql("SELECT * FROM ENGINE_TS where Offset >= 29 ").collect().foreach(println)


    //stop the context
    println("Stopping Spark Context..")
    sc.stop()
  }

}
// scalastyle:on println