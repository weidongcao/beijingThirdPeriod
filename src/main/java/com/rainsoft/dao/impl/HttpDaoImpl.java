package com.rainsoft.dao.impl;

import com.rainsoft.dao.HttpDao;
import com.rainsoft.domain.RegContentHttp;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * Created by CaoWeidong on 2017-06-12.
 */
public class HttpDaoImpl extends JdbcDaoSupport implements HttpDao {
    @Override
    public List<RegContentHttp> getHttpBydate(String date) {
        JdbcTemplate jdbcTemplate = this.getJdbcTemplate();
        jdbcTemplate.setFetchSize(100000);

        String sql = "select * from REG_CONTENT_HTTP where capture_time >= to_date(? ,'yyyy-mm-dd') and capture_time < (to_date(? ,'yyyy-mm-dd') + 1)";

        List<RegContentHttp> list = jdbcTemplate.query(sql, new Object[]{date, date}, new BeanPropertyRowMapper<RegContentHttp>(RegContentHttp.class));

        return list;
    }
}
