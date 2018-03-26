package com.rainsoft.dao.impl;

import com.rainsoft.dao.ImchatDao;
import com.rainsoft.domain.RegContentImChat;
import com.rainsoft.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;
import java.util.Optional;

/**
 * 聊天数据Dao实现层
 * Created by Administrator on 2017-06-12.
 */
public class ImchatDaoImpl extends JdbcDaoSupport implements ImchatDao {
    private static final Logger logger = LoggerFactory.getLogger(ImchatDaoImpl.class);

    private static final String tableName = "REG_CONTENT_IM_CHAT";
    @Override
    public List<RegContentImChat> getImchatBydate(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);

        String templateSql = "select * from Reg_Content_Im_Chat where import_time >= to_date('${date}' ,'yyyy-mm-dd') and import_time < (to_date('${date}' ,'yyyy-mm-dd') + 1)";
        String sql = templateSql.replace("${date}", date);
        logger.info("聊天数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentImChat.class));
    }

    @Override
    public List<String[]> getImChatByHours(String startTime, String endTime) {
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
