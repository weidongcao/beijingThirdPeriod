package com.rainsoft.dao.impl;

import com.rainsoft.dao.WeiboDao;
import com.rainsoft.domain.RegContentWeibo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class WeiboDaoImpl extends JdbcDaoSupport implements WeiboDao {
    private static final Logger logger = LoggerFactory.getLogger(WeiboDaoImpl.class);

    @Override
    public List<RegContentWeibo> getWeiboByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from reg_content_weibo";

        String sql = templeSql.replace("${date}", date);
        logger.info("Weibo数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentWeibo.class));
    }
}
