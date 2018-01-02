package com.rainsoft.spark;

import com.rainsoft.conf.ConfigurationManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
/**
 * Created by CaoWeiDong on 2017-12-05.
 */
public class TestSparkConnectOracle {
    public static void main(String[] args) {
        String oracleDriver = ConfigurationManager.getProperty("oracle.driver");
        String oracleUrl = ConfigurationManager.getProperty("oracle.url");
        String oracleTable = "service_info";
        String oracleUserName = ConfigurationManager.getProperty("oracle.username");
        String oraclePassword = ConfigurationManager.getProperty("oracle.password");
        SparkSession spark = SparkSession.builder()
                .appName(TestSparkConnectOracle.class.getSimpleName())
                .master("local[4]")
                .getOrCreate();

        Dataset<Row> oracleDS = spark.read()
                .format("jdbc")
                .option("driver", oracleDriver)
                .option("url", oracleUrl)
                .option("dbtable", oracleTable)
                .option("user", oracleUserName)
                .option("password", oraclePassword)
                .load();

        oracleDS.createOrReplaceTempView("service_info");

        spark.sql("select * from service_info").show();

    }
}
