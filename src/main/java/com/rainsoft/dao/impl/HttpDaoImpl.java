package com.rainsoft.dao.impl;

import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.dao.HttpDao;
import com.rainsoft.domain.RegContentHttp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.Date;
import java.util.List;

/**
 * HTTP数据DAO
 * Created by CaoWeiDong on 2017-06-12.
 */
public class HttpDaoImpl extends JdbcDaoSupport implements HttpDao {
    private static final Logger logger = LoggerFactory.getLogger(HttpDaoImpl.class);

    @Override
    public List<RegContentHttp> getHttpBydate(String date, float startPercent, float endPercent) {
        JdbcTemplate jdbcTemplate = this.getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);

        String templeSql = "select * from REG_CONTENT_HTTP where capture_time >= (to_date('${date}' ,'yyyy-mm-dd') + ${startPercent}) and capture_time < (to_date('${date}' ,'yyyy-mm-dd') + ${endPercent})";

        String sql = templeSql.replace("${date}", date);
        sql = sql.replace("${startPercent}", startPercent + "");
        sql = sql.replace("${endPercent}", endPercent + "");
        logger.info("HTTP获取Oracle数据sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentHttp.class));
    }
}
