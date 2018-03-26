package com.rainsoft.dao.impl;

import com.rainsoft.dao.RealDao;
import com.rainsoft.domain.RegRealIdInfo;
import com.rainsoft.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;
import java.util.Optional;

/**
 * Oracle真实数据Dao实现层
 * Created by CaoWeiDong on 2017-06-28.
 */
public class RealDaoImpl extends JdbcDaoSupport implements RealDao{
    private static final Logger logger = LoggerFactory.getLogger(RealDaoImpl.class);
    private static final String tableName = "reg_realid_info";
    @Override
    public List<RegRealIdInfo> getRealByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from reg_realid_info";

        String sql = templeSql.replace("${date}", date);
        logger.info("Real数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegRealIdInfo.class));
    }

    @Override
    public List<String[]> getRealByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        String templeSql = "select * from ${tableName} where last_logintime >= to_date('${startTime}' ,'yyyy-mm-dd hh24:mi:ss') and last_logintime < to_date('${endTime}' ,'yyyy-mm-dd hh24:mi:ss')";

        String sql = templeSql.replace("${startTime}", startTime)
                .replace("${endTime}", endTime)
                .replace("${tableName}", tableName);
        logger.info("{} 数据获取Oracle数据sql: {}", tableName, sql);

        /*
         * 返回结果为数组类型的List
         */
        return jdbcTemplate.query(sql, JdbcUtils::resultSetToList);
    }

    /**
     * 根据日期获取在此日期之后最小(最早)的ID
     */
    @Override
    public Optional<Long> getMinId() {
        return JdbcUtils.getMinIdFromDate(getJdbcTemplate(), tableName, Optional.empty());
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
