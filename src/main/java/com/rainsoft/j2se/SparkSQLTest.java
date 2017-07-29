package com.rainsoft.j2se;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * Created by Administrator on 2017-04-26.
 */
public class SparkSQLTest {
    public static void main(String[] args) {
        StructType aaSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("yest_imsi", DataTypes.StringType, true),
                DataTypes.createStructField("hist_imsi", DataTypes.StringType, true),
                DataTypes.createStructField("yest_service_name", DataTypes.StringType, true),
                DataTypes.createStructField("hist_service_name", DataTypes.StringType, true),
                DataTypes.createStructField("yest_service_code", DataTypes.StringType, true),
                DataTypes.createStructField("hist_service_code", DataTypes.StringType, true),
                DataTypes.createStructField("yest_machine_id", DataTypes.StringType, true),
                DataTypes.createStructField("hist_amchine_id", DataTypes.StringType, true),
                DataTypes.createStructField("hist_weight", DataTypes.FloatType, true),
                DataTypes.createStructField("yest_phone7", DataTypes.StringType, true),
                DataTypes.createStructField("yest_area_name", DataTypes.StringType, true),
                DataTypes.createStructField("yest_area_code", DataTypes.StringType, true),
                DataTypes.createStructField("yest_phone_type", DataTypes.StringType, true),
                DataTypes.createStructField("yest_region", DataTypes.StringType, true)
        ));
        String[] aa = aaSchema.fieldNames();
        System.out.println(StringUtils.join(aa, "\t\t"));
    }
}
