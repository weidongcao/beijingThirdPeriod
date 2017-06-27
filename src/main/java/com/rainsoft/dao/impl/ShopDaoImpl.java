package com.rainsoft.dao.impl;

import com.rainsoft.dao.ShopDao;
import com.rainsoft.domain.RegContentShop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ShopDaoImpl extends JdbcDaoSupport implements ShopDao{
    private static final Logger logger = LoggerFactory.getLogger(ShopDaoImpl.class);

    @Override
    public List<RegContentShop> getShopByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "";

        String sql = templeSql.replace("${date}", date);
        logger.info(" sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentShop.class));
    }
}
