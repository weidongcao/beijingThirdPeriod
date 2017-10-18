package com.rainsoft.dao.impl;

import com.rainsoft.dao.SearchDao;
import com.rainsoft.domain.RegContentSearch;
import com.rainsoft.utils.JdbcUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * 购物数据Dao层
 * Created by CaoWeiDong on 2017-06-28.
 */
public class SearchDaoImpl extends JdbcDaoSupport implements SearchDao {
    private static final Logger logger = LoggerFactory.getLogger(SearchDaoImpl.class);

    private static final String tableName = NamingRuleUtils.getOracleContentTableName("search");

    @Override
    public List<RegContentSearch> getSearchByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from reg_content_search where capture_time >= to_date('${date}' ,'yyyy-mm-dd') and capture_time < (to_date('${date}' ,'yyyy-mm-dd') + 1)";

        String sql = templeSql.replace("${date}", date);
        logger.info("Search数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentSearch.class));
    }

    @Override
    public List<String[]> getSearchByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        return JdbcUtils.getDatabaseByPeriod(jdbcTemplate, tableName, startTime, endTime);
    }
}
