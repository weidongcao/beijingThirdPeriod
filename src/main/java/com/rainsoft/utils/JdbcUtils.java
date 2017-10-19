package com.rainsoft.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * JDBC工具类
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
                    Timestamp ts = rs.getTimestamp(index);
                    value = (ts == null ? null : DateFormatUtils.DATE_TIME_FORMAT.format(ts));
                    break;
                case Types.DATE:
                    Date date = rs.getDate(index);
                    value = (date == null ? null : DateFormatUtils.DATE_TIME_FORMAT.format(date));
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
     * @param rs 数据库查询返回结果
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

    /**
     * 根据开始时间和结束时间获取Oracle指定表指定时间段内的数据并返回数组类型的列表
     *
     * @param jdbcTemplate Jdbc连接
     * @param tableName 表名
     * @param startTime 开始时间, 格式：yyyy-MM-dd HH:mm:ss
     * @param endTime 结束时间, 格式：yyyy-MM-dd HH:mm:ss
     * @return Oracle数据 数组列表
     */
    public static List<String[]> getDatabaseByPeriod(JdbcTemplate jdbcTemplate, String tableName, String startTime, String endTime) {
        String templeSql = "select * from ${tableName} where capture_time >= to_date('${startTime}' ,'yyyy-mm-dd hh24:mi:ss') and capture_time < to_date('${endTime}' ,'yyyy-mm-dd hh24:mi:ss')";

        String sql = templeSql.replace("${startTime}", startTime)
                .replace("${endTime}", endTime)
                .replace("${tableName}", tableName);
        logger.info("{} 数据获取Oracle数据sql: {}", tableName, sql);

        /*
         * 返回结果为数组类型的List
         */
        List<String[]> list = jdbcTemplate.query(sql, JdbcUtils::resultSetToList);
        return list;
    }
}
