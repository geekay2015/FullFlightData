package org.kadam.spark;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;


import java.io.Serializable;

/**
 * Created by gangadharkadam on 3/15/16.
 * Project Name: $(PROJECT_NAME}
 */


public class LoadSparkDataFrameToDB implements Serializable {

    // set the Logger
    private static final Logger LOGGER = Logger.getLogger(LoadSparkDataFrameToDB.class);

    // Define the spark Context
    private static final JavaSparkContext sc =
            new JavaSparkContext(
                    new SparkConf()
                            .setAppName("LoadSparkDataFrameToDB")
                            .setMaster("local")
            );

    private static final String MYSQL_DRIVER = "com.mysql.jdbc.driver";
    private static final String MYSQL_CONNECTOR_URL = "jdbc:mysql://localhost:3306/employees";
    private static final String MYSQL_USERNAME = "hive";
    private static final String MYSQL_PWD = "hive";

    private static class DbConnection extends AbstractFunction0<Connection> implements Serializable {
        private final String driverClassName;
        private final String connectionUrl;
        private final String userName;
        private final String passWord;

        public DbConnection() {
            this.driverClassName = LoadSparkDataFrameToDB.MYSQL_DRIVER;
            this.connectionUrl = LoadSparkDataFrameToDB.MYSQL_CONNECTOR_URL;
            this.userName = LoadSparkDataFrameToDB.MYSQL_USERNAME;
            this.passWord = LoadSparkDataFrameToDB.MYSQL_PWD;
        }


        @Override
        public Connection apply() {
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                LOGGER.error("Failed to load driver class", e);
            }

            Properties properties = new Properties();
            properties.setProperty("user", userName);
            properties.setProperty("password", passWord);

            Connection connection = null;

            try {
                connection = DriverManager.getConnection(connectionUrl, properties);
            } catch (SQLException e) {
                LOGGER.error("Connection failed", e);
            }
            return connection;

        }
    }

    private static class MapResult extends AbstractFunction1<ResultSet, Object[]> implements Serializable {
        public Object[] apply(ResultSet row) {
            return JdbcRDD.resultSetToObjectArray(row);
        }
    }

    public static void main(String[] args) {
        DbConnection dbConnection = new DbConnection();

        // Load data from mySQL
        JdbcRDD<Object[]> jdbcRDD = new JdbcRDD<>(sc.sc(), dbConnection,
                "select * from employees where  emp_no >= ? and emp_no <= ?", 10001,499999, 10,
                new MapResult(),ClassManifestFactory$.MODULE$.fromClass(Object[].class));

        // Convert to javaRDD
        JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(Object[].class));


        // Join the first name and last name
        List<String> employeeFullNameList = javaRDD.map(record -> record[2] + " " + record[3]).collect();
        employeeFullNameList.forEach(LOGGER::info);


    }


}
