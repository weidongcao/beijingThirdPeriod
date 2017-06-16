package com.rainsoft.dao.impl;

import com.rainsoft.dao.FtpDao;
import com.rainsoft.domain.RegContentFtp;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by CaoWeidong on 2017-06-12.
 */
public class FtpDaoImpl extends JdbcDaoSupport implements FtpDao {

    @Override
    public List<RegContentFtp> getFtpBydate(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000000);

        String sql = "select * from REG_CONTENT_FTP where capture_time >= to_date(? ,'yyyy-mm-dd') and capture_time < (to_date(? ,'yyyy-mm-dd') + 1)";


        List<RegContentFtp> list = jdbcTemplate.query(sql, new Object[]{date, date}, new BeanPropertyRowMapper<RegContentFtp>(RegContentFtp.class));

        return list;
    }


    @Override
    public List<String> getFtpFieldValueByTime(String date) {

        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setResultsMapCaseInsensitive(true);
        jdbcTemplate.setFetchSize(1000);
        String sql = "select 1 from dual";

        List<String> temp = jdbcTemplate.query(sql, new ResultSetExtractor<List<String>>() {
            @Override
            public List<String> extractData(ResultSet rs) throws SQLException, DataAccessException {
                List<String> datas = new ArrayList<>();
                StringBuilder sb = new StringBuilder();
                while (rs.next()) {
                    for (int i = 0; i < 23; i++) {
                        sb.append(rs.getString(i));
                        if (i < 23) {
                            sb.append("\t");
                        }
                    }
                    datas.add(sb.toString());
                    sb.delete(0, sb.length());
                }

                return datas;
            }
        });
        return temp;
    }
}
