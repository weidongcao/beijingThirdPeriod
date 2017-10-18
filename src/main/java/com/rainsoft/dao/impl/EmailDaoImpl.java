package com.rainsoft.dao.impl;

import com.rainsoft.dao.EmailDao;
import com.rainsoft.domain.RegContentEmail;
import com.rainsoft.utils.JdbcUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * 邮件数据Dao层实现类
 * Created by CaoWeiDong on 2017-06-28.
 */
public class EmailDaoImpl extends JdbcDaoSupport implements EmailDao{
    private static final Logger logger = LoggerFactory.getLogger(EmailDaoImpl.class);
    private static final String tableName = NamingRuleUtils.getOracleContentTableName("email");

    @Override
    public List<RegContentEmail> getEmailByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from reg_content_email where capture_time >= to_date('${date}' ,'yyyy-mm-dd') and capture_time < (to_date('${date}' ,'yyyy-mm-dd') + 1)";

        String sql = templeSql.replace("${date}", date);
        logger.info("Email数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentEmail.class));
    }

    @Override
    public List<String[]> getEmailByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        return JdbcUtils.getDatabaseByPeriod(jdbcTemplate, tableName, startTime, endTime);
    }
}
