package com.rainsoft.dao.impl;

import com.rainsoft.dao.FtpDao;
import com.rainsoft.domain.RegContentFtp;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Created by CaoWeidong on 2017-06-12.
 */
public class FtpDaoImpl extends JdbcDaoSupport implements FtpDao {
    private static final Logger logger = LoggerFactory.getLogger(FtpDaoImpl.class);

    @Override
    public List<RegContentFtp> getFtpBydate(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from REG_CONTENT_FTP where capture_time >= to_date('${date}' ,'yyyy-mm-dd') and capture_time < (to_date('${date}' ,'yyyy-mm-dd') + 1)";

        String sql = templeSql.replace("${date}", date);
        logger.info("FTP数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentFtp.class));
    }

    @Override
    public List<String[]> getFtpByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from REG_CONTENT_FTP where capture_time >= to_date('${startTime}' ,'yyyy-mm-dd hh24:mi:ss') and capture_time < to_date('${endTime}' ,'yyyy-mm-dd hh24:mi:ss')";

        String sql = templeSql.replace("${startTime}", startTime)
                .replace("${endTime}", endTime);
        logger.info("FTP数据获取Oracle数据sql: {}", sql);

        /**
         * 返回结果为数组类型的List
         */
        List<String[]> list = jdbcTemplate.query(sql, new ResultSetExtractor<List<String[]>>() {
            @Override
            public List<String[]> extractData(ResultSet rs) throws SQLException, DataAccessException {
                return JdbcUtils.resultSetToList(rs);
            }
        });
        return list;
    }

    @Override
    public List<String> getFtpFieldValueByTime(String date) {

        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setResultsMapCaseInsensitive(true);
        jdbcTemplate.setFetchSize(100);
        String sql = "select * from REG_CONTENT_FTP where capture_time >= to_date(? ,'yyyy-mm-dd') and capture_time < (to_date(? ,'yyyy-mm-dd') + 1) and rownum < 2";

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

    @Override
    public RegContentFtp getFtpById(int id) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);

        String sql = "select * from REG_CONTENT_FTP where id = ?";


        List<RegContentFtp> list = jdbcTemplate.query(sql, new Object[]{id}, new BeanPropertyRowMapper<RegContentFtp>(RegContentFtp.class));

        return list.get(0);
    }
}
