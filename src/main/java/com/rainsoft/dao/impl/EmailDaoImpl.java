package com.rainsoft.dao.impl;

import com.rainsoft.dao.EmailDao;
import com.rainsoft.domain.RegContentEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class EmailDaoImpl extends JdbcDaoSupport implements EmailDao{
    private static final Logger logger = LoggerFactory.getLogger(EmailDaoImpl.class);

    @Override
    public List<RegContentEmail> getEmailByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "";

        String sql = templeSql.replace("${date}", date);
        logger.info(" sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentEmail.class));
    }
}
