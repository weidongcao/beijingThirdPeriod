package com.rainsoft.dao.impl;

import com.rainsoft.dao.HttpDao;
import com.rainsoft.domain.RegContentHttp;
import com.rainsoft.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;
import java.util.Optional;

/**
 * HTTP数据DAO
 * Created by CaoWeiDong on 2017-06-12.
 */
public class HttpDaoImpl extends JdbcDaoSupport implements HttpDao {
    private static final Logger logger = LoggerFactory.getLogger(HttpDaoImpl.class);

    private static final String tableName = "REG_CONTENT_HTTP";
    @Override
    public List<RegContentHttp> getHttpBydate(String date, float startPercent, float endPercent) {
        JdbcTemplate jdbcTemplate = this.getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);

        String templeSql = "select * from ${tableName} where import_time >= (to_date('${date}' ,'yyyy-mm-dd') + ${startPercent}) and import_time < (to_date('${date}' ,'yyyy-mm-dd') + ${endPercent})";

        String sql = templeSql.replace("${date}", date);
        sql = sql.replace("${startPercent}", startPercent + "");
        sql = sql.replace("${endPercent}", endPercent + "");
        logger.info("HTTP获取Oracle数据sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentHttp.class));
    }
    @Override
    public List<String[]> getHttpByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(100);
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
