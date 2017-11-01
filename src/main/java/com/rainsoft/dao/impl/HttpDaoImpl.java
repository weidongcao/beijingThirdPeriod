package com.rainsoft.dao.impl;

import com.rainsoft.dao.HttpDao;
import com.rainsoft.domain.RegContentHttp;
import com.rainsoft.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * HTTP数据DAO
 * Created by CaoWeiDong on 2017-06-12.
 */
public class HttpDaoImpl extends JdbcDaoSupport implements HttpDao {
    private static final Logger logger = LoggerFactory.getLogger(HttpDaoImpl.class);

    private static final String tableName = "REG_CONTENT_HTTP";
    @Override
    public List<RegContentHttp> getHttpBydate(String date, float startPercent, float endPercent) {
        JdbcTemplate jdbcTemplate = this.getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);

        String templeSql = "select * from ${tableName} where import_time >= (to_date('${date}' ,'yyyy-mm-dd') + ${startPercent}) and import_time < (to_date('${date}' ,'yyyy-mm-dd') + ${endPercent})";

        String sql = templeSql.replace("${date}", date);
        sql = sql.replace("${startPercent}", startPercent + "");
        sql = sql.replace("${endPercent}", endPercent + "");
        logger.info("HTTP获取Oracle数据sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentHttp.class));
    }
    @Override
    public List<String[]> getHttpByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(100);
        String templeSql = "select * from ${tableName} where import_time >= to_date('${startTime}' ,'yyyy-mm-dd hh24:mi:ss') and import_time < to_date('${endTime}' ,'yyyy-mm-dd hh24:mi:ss')";

        String sql = templeSql.replace("${startTime}", startTime)
                .replace("${endTime}", endTime)
                .replace("${tableName}", tableName);
        logger.info("{} 数据获取Oracle数据sql: {}", tableName, sql);

        /*
         * 返回结果为数组类型的List
         */
        return jdbcTemplate.query(sql, JdbcUtils::resultSetToList);
    }


}
