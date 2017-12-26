package com.rainsoft.test

import com.rainsoft.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by CaoWeiDong on 2017-12-19.
  */
object Spark2ConnectOracle {
    def main(args: Array[String]): Unit = {
        val tableName = "ANALYSIS"

        val spark = SparkSession.builder()
            .appName(Spark2ConnectOracle.getClass.getSimpleName)
            .master("local[2]")
            .getOrCreate()

        val dataDF = spark.read
            .schema(DataTypes.createStructType(Array(
                DataTypes.createStructField("MAC", StringType, true),
                DataTypes.createStructField("COMMUNITY", StringType, true)
            )))
            .csv("file:///E:\\data\\track-small.csv")

//        SparkUtils.writeIntoOracle(dataDF, tableName)
    }
}
