package com.rainsoft.dao.impl;

import com.rainsoft.dao.EmailDao;
import com.rainsoft.domain.RegContentEmail;
import com.rainsoft.utils.JdbcUtils;
import com.rainsoft.utils.NamingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;
import java.util.Optional;

/**
 * 邮件数据Dao层实现类
 * Created by CaoWeiDong on 2017-06-28.
 */
public class EmailDaoImpl extends JdbcDaoSupport implements EmailDao{
    private static final Logger logger = LoggerFactory.getLogger(EmailDaoImpl.class);
    private static final String tableName = NamingUtils.getTableName("email");

    @Override
    public List<RegContentEmail> getEmailByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from reg_content_email where import_time >= to_date('${date}' ,'yyyy-mm-dd') and import_time < (to_date('${date}' ,'yyyy-mm-dd') + 1)";

        String sql = templeSql.replace("${date}", date);
        logger.info("Email数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentEmail.class));
    }

    @Override
    public List<String[]> getEmailByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        return JdbcUtils.getDatabaseByPeriod(jdbcTemplate, tableName, startTime, endTime);
    }

    /**
     * 根据日期获取在此日期之后最小(最早)的ID
     * @param date 起始日期
     */
    @Override
    public Optional<Long> getMinIdFromDate(Optional<String> date) {
        return JdbcUtils.getMinIdFromDate(getJdbcTemplate(), tableName, date);
    }

    /**
     * 根据指定的ID获取从此ID开始指定数量的数据
     * Oracle内容表的ID是序列自动生成的，是递增的，
     * 通过此方式可以获取到最新的数据
     * @param id 起始ID
     */
    @Override
    public List<String[]> getDatasByStartIDWithStep(Optional<Long> id) {
        return JdbcUtils.getDataById(getJdbcTemplate(), tableName, id);
    }
}
