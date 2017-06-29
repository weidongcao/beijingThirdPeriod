package com.rainsoft.dao.impl;

import com.rainsoft.dao.SearchDao;
import com.rainsoft.domain.RegContentSearch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class SearchDaoImpl extends JdbcDaoSupport implements SearchDao {
    private static final Logger logger = LoggerFactory.getLogger(SearchDaoImpl.class);

    @Override
    public List<RegContentSearch> getSearchByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from reg_content_search";

        String sql = templeSql.replace("${date}", date);
        logger.info(" sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentSearch.class));
    }
}