package com.rainsoft.dao.impl;

import com.rainsoft.dao.ShopDao;
import com.rainsoft.domain.RegContentShop;
import com.rainsoft.utils.JdbcUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * 购物数据Dao实现类
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ShopDaoImpl extends JdbcDaoSupport implements ShopDao{
    private static final Logger logger = LoggerFactory.getLogger(ShopDaoImpl.class);

    private static final String tableName = NamingRuleUtils.getOracleContentTableName("shop");
    @Override
    public List<RegContentShop> getShopByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from reg_content_shop where import_time >= to_date('${date}' ,'yyyy-mm-dd') and import_time < (to_date('${date}' ,'yyyy-mm-dd') + 1)";

        String sql = templeSql.replace("${date}", date);
        logger.info("Shop数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentShop.class));
    }

    @Override
    public List<String[]> getShopByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        return JdbcUtils.getDatabaseByPeriod(jdbcTemplate, tableName, startTime, endTime);
    }
}
