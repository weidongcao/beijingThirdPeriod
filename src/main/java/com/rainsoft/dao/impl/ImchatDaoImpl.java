package com.rainsoft.dao.impl;

import com.rainsoft.dao.ImchatDao;
import com.rainsoft.domain.RegContentImChat;
import com.rainsoft.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 聊天数据Dao实现层
 * Created by Administrator on 2017-06-12.
 */
public class ImchatDaoImpl extends JdbcDaoSupport implements ImchatDao {
    private static final Logger logger = LoggerFactory.getLogger(ImchatDaoImpl.class);

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
        String templeSql = "select * from REG_CONTENT_IM_CHAT where import_time >= to_date('${startTime}' ,'yyyy-mm-dd hh24:mi:ss') and import_time < to_date('${endTime}' ,'yyyy-mm-dd hh24:mi:ss')";

        String sql = templeSql.replace("${startTime}", startTime)
                .replace("${endTime}", endTime);
        logger.info("im_chat 数据获取Oracle数据sql: {}", sql);

        /*
         * 返回结果为数组类型的List
         */
        return jdbcTemplate.query(sql, JdbcUtils::resultSetToList);
    }
}
