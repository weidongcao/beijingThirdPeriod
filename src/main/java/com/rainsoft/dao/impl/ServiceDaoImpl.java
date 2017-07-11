package com.rainsoft.dao.impl;

import com.rainsoft.dao.ServiceDao;
import com.rainsoft.domain.ServiceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ServiceDaoImpl extends JdbcDaoSupport implements ServiceDao {
    private static final Logger logger = LoggerFactory.getLogger(SearchDaoImpl.class);

    @Override
    public List<ServiceInfo> getServiceByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from service_info";

        String sql = templeSql.replace("${date}", date);
        logger.info("Service数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(ServiceInfo.class));
    }
}
