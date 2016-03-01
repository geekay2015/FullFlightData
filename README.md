# FullFlightData
spark-submit \
--master local[*] \
--class org.kadam.spark.GetEngineInformationCSVDataToHDFS \
/Users/gangadharkadam/myapps/FullFlightData/target/scala-2.11/FullFlightData-assembly-1.0.jar \
file:///Users/gangadharkadam/myapps/FullFlightData/src/main/resources/engineInformation.csv \
/user/hive/warehouse/ffd_data/


spark-submit \
--master local[*] \
--class org.kadam.spark.GetEngineTimeSeriesCSVDataToHDFS \
/Users/gangadharkadam/myapps/FullFlightData/target/scala-2.11/FullFlightData-assembly-1.0.jar \
file:///Users/gangadharkadam/myapps/FullFlightData/src/main/resources/engineTimeSeries.csv \
/user/hive/warehouse/ffd_ts/ 
