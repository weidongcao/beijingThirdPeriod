package com.rainsoft.utils

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

/**
  * Created by CaoWeiDong on 2017-12-19.
  */
class OracleJdbcDialects extends JdbcDialect{
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle") || url.contains("oracle")

    override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
        case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
        case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.NUMERIC))
        case IntegerType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
        case LongType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
        case DoubleType => Some(JdbcType("NUMBER(16,4)", java.sql.Types.NUMERIC))
        case FloatType => Some(JdbcType("NUMBER(16,4)", java.sql.Types.NUMERIC))
        case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.NUMERIC))
        case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.NUMERIC))
        case BinaryType => Some(JdbcType("BLOB", java.sql.Types.BLOB))
        case TimestampType => Some(JdbcType("DATE", java.sql.Types.DATE))
        case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
        //        case DecimalType.Fixed(precision, scale) => Some(JdbcType("NUMBER(" + precision + "," + scale + ")", java.sql.Types.NUMERIC))
        case DecimalType.SYSTEM_DEFAULT => Some(JdbcType("NUMBER(38,2)", java.sql.Types.NUMERIC))
        case _ => None
    }
}
