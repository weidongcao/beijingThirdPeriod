package com.rainsoft.utils;

import com.rainsoft.FieldConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by CaoWeiDong on 2017-08-06.
 */
public class JdbcUtils {
    private static final Logger logger = LoggerFactory.getLogger(JdbcUtils.class);

    public static String getFieldValue(ResultSet rs, int type, int index) {
        String value = null;
        try {
            switch (type) {
                case Types.VARCHAR:
                    value = rs.getString(index);
                    break;
                case Types.INTEGER:
                    value = rs.getInt(index) + "";
                    break;
                case Types.BIGINT:
                    value = rs.getLong(type) + "";
                    break;
                case Types.DECIMAL:
                    value = rs.getBigDecimal(index) + "";
                    break;
                case Types.DOUBLE:
                    value = rs.getDouble(index) + "";
                    break;
                case Types.FLOAT:
                    value = rs.getFloat(index) + "";
                    break;
                case Types.TIMESTAMP:
                    value = DateUtils.TIME_FORMAT.format(rs.getTimestamp(index));
                    break;
                case Types.DATE:
                    value = DateUtils.TIME_FORMAT.format(rs.getDate(index));
                    break;
                case Types.CHAR:
                    value = rs.getString(index);
                    break;
                case Types.NUMERIC:
                    value = rs.getBigDecimal(index) + "";
                    break;
            }
            if (null == value) {
                value = "";
            } else {
                value = value.replace("\t", "").replace("\r\n", "").replace("\n", "").replace("\r", "");
            }
        } catch (SQLException e) {
            logger.info("获取JDBC字段值出错所在下标：{}", type);
            e.printStackTrace();
        }
        return value;
    }

    /**
     * 将从Oracle查询出来的结果转为List<String[]>>的格式
     * @param rs
     * @return
     * @throws SQLException
     */
    public static List<String[]> resultSetToList(ResultSet rs) throws SQLException {
        List<String[]> list = new ArrayList<>();
        ResultSetMetaData rsm = rs.getMetaData();
        int colSize = rsm.getColumnCount();
        while (rs.next()) {
            String[] line = new String[colSize];
            for (int i = 1; i <= colSize; i++) {
                int type = rsm.getColumnType(i);
                line[i - 1] = JdbcUtils.getFieldValue(rs, type, i);
            }
            list.add(line);
        }
        return list;

    }
}
