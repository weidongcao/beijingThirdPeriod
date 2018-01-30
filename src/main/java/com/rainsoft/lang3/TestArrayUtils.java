package com.rainsoft.lang3;

import com.rainsoft.FieldConstants;
import com.rainsoft.utils.NamingRuleUtils;
import org.apache.commons.lang.ArrayUtils;

/**
 * Created by Administrator on 2017-10-18.
 */
public class TestArrayUtils {
    public static void main(String[] args) {

        String[] columns = FieldConstants.ORACLE_TABLE_COLUMN_MAP.get(NamingRuleUtils.getOracleContentTableName("ftp"));

        int captureTimeIndex = ArrayUtils.indexOf(columns, "aaaaaaaa");
        System.out.println("captureTimeIndex = " + captureTimeIndex);
        System.out.println(columns[-1]);
    }
}
