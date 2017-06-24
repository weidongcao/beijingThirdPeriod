package com.rainsoft.dao.impl;

import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.dao.HttpDao;
import com.rainsoft.domain.RegContentHttp;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.Date;
import java.util.List;

/**
 * Created by CaoWeidong on 2017-06-12.
 */
public class HttpDaoImpl extends JdbcDaoSupport implements HttpDao {
    @Override
    public List<RegContentHttp> getHttpBydate(String date, float startPercent, float endPercent) {
        JdbcTemplate jdbcTemplate = this.getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);

        String sql = ConfigurationManager.getProperty("sql_http_get_by_date");
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " date= " + date + "; startPercent= " + startPercent + "; endPercent" + endPercent);

        List<RegContentHttp> list = jdbcTemplate.query(sql, new Object[]{date, startPercent, date, endPercent}, new BeanPropertyRowMapper<>(RegContentHttp.class));

        return list;
    }
}
