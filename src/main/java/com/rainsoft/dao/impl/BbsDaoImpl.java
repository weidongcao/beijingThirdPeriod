package com.rainsoft.dao.impl;

import com.rainsoft.dao.BbsDao;
import com.rainsoft.domain.RegContentBbs;
import com.rainsoft.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * Bbs论坛Dao层
 * Created by CaoWeiDong on 2017-06-28.
 */
public class BbsDaoImpl extends JdbcDaoSupport implements BbsDao {

    private static final Logger logger = LoggerFactory.getLogger(BbsDaoImpl.class);

    private static final String tableName = "REG_CONTENT_BBS";

    @Override
    public List<RegContentBbs> getBbsByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        String templeSql = "select * from ${tableName} where capture_time >= to_date('${date}' ,'yyyy-mm-dd') and capture_time < (to_date('${date}' ,'yyyy-mm-dd') + 1)";

        String sql = templeSql.replace("${date}", date)
                .replace("${tableName}", tableName);
        logger.info("Bbs数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentBbs.class));
    }

    /**
     * 查询指定开始时间和结束时间之间的数据并返回列表
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 查询返回结果
     */
    @Override
    public List<String[]> getBbsByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        return JdbcUtils.getDatabaseByPeriod(jdbcTemplate, tableName, startTime, endTime);
    }
}
