package com.rainsoft.dao.impl;

import com.rainsoft.dao.RealDao;
import com.rainsoft.domain.RegRealIdInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class RealDaoImpl extends JdbcDaoSupport implements RealDao{
    private static final Logger logger = LoggerFactory.getLogger(RealDaoImpl.class);
    @Override
    public List<RegRealIdInfo> getRealByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from reg_realid_info";

        String sql = templeSql.replace("${date}", date);
        logger.info("Real数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegRealIdInfo.class));
    }
}
