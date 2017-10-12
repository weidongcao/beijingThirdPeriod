package com.rainsoft.dao.impl;

import com.rainsoft.dao.BbsDao;
import com.rainsoft.domain.RegContentBbs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * Bbs论坛Dao层
 * Created by CaoWeiDong on 2017-06-28.
 */
public class BbsDaoImpl extends JdbcDaoSupport implements BbsDao {

    private static final Logger logger = LoggerFactory.getLogger(BbsDaoImpl.class);

    @Override
    public List<RegContentBbs> getBbsByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from reg_content_bbs where capture_time >= to_date('${date}' ,'yyyy-mm-dd') and capture_time < (to_date('${date}' ,'yyyy-mm-dd') + 1)";

        String sql = templeSql.replace("${date}", date);
        logger.info("Bbs数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentBbs.class));
    }
}
