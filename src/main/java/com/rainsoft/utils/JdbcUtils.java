package com.rainsoft.utils;

import com.rainsoft.conf.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.*;
import java.text.ParseException;
import java.util.*;
import java.util.Date;

/**
 * JDBC工具类
 * Created by CaoWeiDong on 2017-08-06.
 */
public class JdbcUtils {
    private static final Logger logger = LoggerFactory.getLogger(JdbcUtils.class);
    //获取Oracle表最小ID的SQL模板
    private static final String getMinIdSqlTemplate = "select min(id) from ${tablename} ";
    //import_time大于指定日期的条件
    private static final String importTimeGtConditionTemplate = " import_time >= to_date('${import_time}', 'yyyy-mm-dd')";
    //从Oracle表指定的ID开始抽取指定的数据量
    private static final String selectByIdsqlTemplqte = "select * from ${tableName} where id >= ${id} and rownum <= ${num}";
    //根据开始时间和结束时间从指定的表中查询数据
    private static String getDataByTimeSqlTemplate = "select * from ${tableName} where last_logintime >= to_date('${startTime}' ,'yyyy-mm-dd hh24:mi:ss') and last_logintime < to_date('${endTime}' ,'yyyy-mm-dd hh24:mi:ss')";
    //获取指定表指定字段的最小值
    private static String getMinValueSqlTemplate = "select min(${fieldName}) from ${tableName}";

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
//                    value = (ts == null ? null : DateFormatUtils.SOLR_FORMAT.format(ts));
                    value = (ts == null ? null : DateFormatUtils.DATE_TIME_FORMAT.format(ts));
                    break;
                case Types.DATE:
                    Date date = rs.getDate(index);
//                    value = (date == null ? null : DateFormatUtils.SOLR_FORMAT.format(date));
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
     *
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
     * 获取指定的开始时间和结束时间之间的数据
     *
     * @param jdbcTemplate Spring Jdbc
     * @param tableName    表名
     * @param startTime    开始时间
     * @param endTime      结束时间
     * @return 返回结果
     */
    public static List<String[]> getDataByTime(JdbcTemplate jdbcTemplate, String tableName, String startTime, String endTime) {
        String sql = getDataByTimeSqlTemplate.replace("${startTime}", startTime)
                .replace("${endTime}", endTime)
                .replace("${tableName}", tableName);
        logger.info("{} 数据获取Oracle数据sql: {}", tableName, sql);

        // 返回结果为数组类型的List
        return jdbcTemplate.query(sql, JdbcUtils::resultSetToList);

    }

    /**
     * 根据开始时间和结束时间获取Oracle指定表指定时间段内的数据并返回数组类型的列表
     *
     * @param jdbcTemplate Jdbc连接
     * @param tableName    表名
     * @param startTime    开始时间, 格式：yyyy-MM-dd HH:mm:ss
     * @param endTime      结束时间, 格式：yyyy-MM-dd HH:mm:ss
     * @return Oracle数据 数组列表
     */
    public static List<String[]> getDatabaseByPeriod(JdbcTemplate jdbcTemplate, String tableName, String startTime, String endTime) {
        String oracleConditionTime = ConfigurationManager.getProperty("oracle.condition.time");
        //内容表与场所表join，获取内容表字段及场所表场所名
        String templeSql = "select content.*, service.service_name from ${tableName} content left join service_info service on content.service_code = service.service_code where content.${oracleConditionTime} >= to_date('${startTime}' ,'yyyy-mm-dd hh24:mi:ss') and content.${oracleConditionTime} < to_date('${endTime}' ,'yyyy-mm-dd hh24:mi:ss')";

        String sql = templeSql.replace("${oracleConditionTime}", oracleConditionTime)
                .replace("${startTime}", startTime)
                .replace("${endTime}", endTime)
                .replace("${tableName}", tableName);
        logger.info("{} 数据获取Oracle数据sql: {}", tableName, sql);

        /*
         * 返回结果为数组类型的List
         */
        return jdbcTemplate.query(sql, JdbcUtils::resultSetToList);
    }

    /**
     * 根据Oracle表名及日期获取从指定日期开始最小(最早)的ID
     * 如果日期为空，则获取该表最小的ID
     *
     * @param jdbcTemplate JdbcTemplate
     * @param tableName    Oracle表名
     * @param optional     一个指定的Java日期
     * @return 最小ID
     */
    public static Optional<Long> getMinIdFromDate(JdbcTemplate jdbcTemplate, String tableName, Optional<String> optional) {
        //拼装查询SQL
        String sql = getMinIdSqlTemplate.replace("${tablename}", tableName);

        //如果日期不为空，则添加日期的条件
        if (optional.isPresent()) {
            sql += " where " + importTimeGtConditionTemplate.replace("${import_time}", optional.get());
            logger.info("查询从 {} 起 {} 表最小的ID的Sql为：{}", optional.get(), tableName, sql);
        } else {
            logger.info("查询 {} 表最小的ID的Sql为：{}", tableName, sql);
        }
        Long minID = jdbcTemplate.queryForObject(sql, Long.class);

        minID = (minID != null) ? minID - 1 : 0;
        logger.info("最小的ID为 {}", minID);

        return Optional.of(minID);
    }

    /**
     * 根据Spring JDBCTemple， Oracle表名，字段名获取该字段最小日期
     * @param jdbcTemplate  Spring JdbcTemplate
     * @param tableName     Oracle表名
     * @param fieldName     字段名
     * @return      最小日期
     */
    public static Optional<Date> getMinTime(JdbcTemplate jdbcTemplate, String tableName, String fieldName) {
        Optional<String> optional = JdbcUtils.getMinValue(jdbcTemplate, tableName,fieldName);
        Optional<Date> date;
        try {
            Date d = DateUtils.parseDate(optional.get(), "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.S");
            d = DateUtils.truncate(d, Calendar.MINUTE);
            date = Optional.of(d);
        } catch (ParseException e) {
            date = Optional.empty();
        }
        return date;
    }
    /**
     * 获取指定表指定字段的最小值
     * 如果日期为空，则获取该表最小的ID
     *
     * @param jdbcTemplate JdbcTemplate
     * @param tableName    Oracle表名
     * @param fieldName     字段名
     * @return 最小值
     */
    public static Optional<String> getMinValue(JdbcTemplate jdbcTemplate, String tableName, String fieldName) {
        //拼装查询SQL
        String sql = getMinValueSqlTemplate.replace("${tableName}", tableName)
                .replace("${fieldName}", fieldName);

        logger.info("查询 {} 表 {} 字段最小值的Sql为：{}", tableName, fieldName, sql);
        String fieldValue = jdbcTemplate.queryForObject(sql, String.class);

        return Optional.ofNullable(fieldValue);
    }

    /**
     * 根据指定的ID获取从此ID开始指定数量的数据
     * Oracle内容表的ID是序列自动生成的，是递增的，
     * 通过此方式可以获取到最新的数据
     *
     * @param id Oracle表ID
     * @return List<String   [   ]>
     */
    public static List<String[]> getDataById(JdbcTemplate jdbcTemplate, String tableName, Optional<Long> id) {
        //一次从Oracle中抽取多少数据（在application.properties配置文件中配置）
        Integer num = ConfigurationManager.getInteger("extract.oracle.count");

        String sql = selectByIdsqlTemplqte.replace("${tableName}", tableName)
                .replace("${id}", id.get() + "")
                .replace("${num}", num + "");

        logger.info("从 {} 表抽取数据sql: {}", tableName, sql);

        //将查询返回结果封装为List，字段值类型传问为String
        return jdbcTemplate.query(sql, JdbcUtils::resultSetToList);
    }
}
