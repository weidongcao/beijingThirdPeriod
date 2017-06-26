package com.rainsoft.dao.impl;

import com.rainsoft.dao.ImchatDao;
import com.rainsoft.domain.RegContentImChat;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.Date;
import java.util.List;

/**
 * Created by Administrator on 2017-06-12.
 */
public class ImchatDaoImpl extends JdbcDaoSupport implements ImchatDao {
    @Override
    public List<RegContentImChat> getImchatBydate(String date) {
                JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);

        String templateSql = "select * from Reg_Content_Im_Chat where capture_time >= to_date(${date} ,'yyyy-mm-dd') and capture_time < (to_date(${date} ,'yyyy-mm-dd') + 1)";
        String sql = templateSql.replace("${date}", date);
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 聊天数据获取一天数据的sql: " + sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentImChat.class));
    }
}
