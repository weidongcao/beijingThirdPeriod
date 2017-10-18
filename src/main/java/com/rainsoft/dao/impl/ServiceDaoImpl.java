package com.rainsoft.dao.impl;

import com.rainsoft.dao.ServiceDao;
import com.rainsoft.domain.ServiceInfo;
import com.rainsoft.utils.JdbcUtils;
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

    private static final String tableName = "service_info";
    @Override
    public List<ServiceInfo> getServiceByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from service_info";

        String sql = templeSql.replace("${date}", date);
        logger.info("Service数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(ServiceInfo.class));
    }

    @Override
    public List<String[]> getServiceByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        String templeSql = "select * from ${tableName} where install_time >= to_date('${startTime}' ,'yyyy-mm-dd hh24:mi:ss') and install_time < to_date('${endTime}' ,'yyyy-mm-dd hh24:mi:ss')";

        String sql = templeSql.replace("${startTime}", startTime)
                .replace("${endTime}", endTime)
                .replace("${tableName}", tableName);
        logger.info("{} 数据获取Oracle数据sql: {}", tableName, sql);

        /**
         * 返回结果为数组类型的List
         */
        List<String[]> list = jdbcTemplate.query(sql, rs -> {
            return JdbcUtils.resultSetToList(rs);
        });
        return list;
    }
}
