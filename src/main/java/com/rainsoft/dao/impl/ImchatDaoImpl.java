package com.rainsoft.dao.impl;

import com.rainsoft.dao.ImchatDao;
import com.rainsoft.domain.RegContentImChat;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * Created by Administrator on 2017-06-12.
 */
public class ImchatDaoImpl extends JdbcDaoSupport implements ImchatDao {
    @Override
    public List<RegContentImChat> getImchatBydate(String date) {
                JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);

        String sql = "select * from Reg_Content_Im_Chat where capture_time >= to_date(? ,'yyyy-mm-dd') and capture_time < (to_date(? ,'yyyy-mm-dd') + 1)";


        List<RegContentImChat> list = jdbcTemplate.query(sql, new Object[]{date, date}, new BeanPropertyRowMapper<>(RegContentImChat.class));

        return list;
    }
}
