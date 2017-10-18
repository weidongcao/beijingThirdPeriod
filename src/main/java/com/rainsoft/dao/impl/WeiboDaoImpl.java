package com.rainsoft.dao.impl;

import com.rainsoft.dao.WeiboDao;
import com.rainsoft.domain.RegContentWeibo;
import com.rainsoft.utils.JdbcUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * 微博数据Dao层实现类
 * Created by CaoWeiDong on 2017-06-28.
 */
public class WeiboDaoImpl extends JdbcDaoSupport implements WeiboDao {
    private static final Logger logger = LoggerFactory.getLogger(WeiboDaoImpl.class);

    private static final String tableName = NamingRuleUtils.getOracleContentTableName("weibo");

    @Override
    public List<RegContentWeibo> getWeiboByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from reg_content_weibo where capture_time >= to_date('${date}' ,'yyyy-mm-dd') and capture_time < (to_date('${date}' ,'yyyy-mm-dd') + 1)";

        String sql = templeSql.replace("${date}", date);
        logger.info("Weibo数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentWeibo.class));
    }

    @Override
    public List<String[]> getWeiboByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        return JdbcUtils.getDatabaseByPeriod(jdbcTemplate, tableName, startTime, endTime);
    }
}
