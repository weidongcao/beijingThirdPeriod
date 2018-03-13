package com.rainsoft.dao.impl;

import com.google.common.base.Optional;
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
        String templeSql = "select * from reg_content_search where import_time >= to_date('${date}' ,'yyyy-mm-dd') and import_time < (to_date('${date}' ,'yyyy-mm-dd') + 1)";

        String sql = templeSql.replace("${date}", date);
        logger.info("Search数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentSearch.class));
    }

    @Override
    public List<String[]> getSearchByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        return JdbcUtils.getDatabaseByPeriod(jdbcTemplate, tableName, startTime, endTime);
    }
    /**
     * 根据日期获取在此日期之后最小(最早)的ID
     * @param date
     * @return
     */
    @Override
    public Optional<Long> getMinIdFromDate(Optional<String> date) {
        return JdbcUtils.getMinIdFromDate(getJdbcTemplate(), tableName, date);
    }

    /**
     * 根据指定的ID获取从此ID开始指定数量的数据
     * Oracle内容表的ID是序列自动生成的，是递增的，
     * 通过此方式可以获取到最新的数据
     * @param id
     * @return
     */
    @Override
    public List<String[]> getDataById(Optional<Long> id) {
        return JdbcUtils.getDataById(getJdbcTemplate(), tableName, id);
    }
}
