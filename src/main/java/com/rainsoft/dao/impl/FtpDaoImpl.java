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
 * Oracle数据库ftp数据Dao实现层
 * Created by CaoWeidong on 2017-06-12.
 */
public class FtpDaoImpl extends JdbcDaoSupport implements FtpDao {
    private static final Logger logger = LoggerFactory.getLogger(FtpDaoImpl.class);

    private static final String tableName = "REG_CONTENT_FTP";
    @Override
    public List<RegContentFtp> getFtpBydate(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from REG_CONTENT_FTP where import_time >= to_date('${date}' ,'yyyy-mm-dd') and import_time < (to_date('${date}' ,'yyyy-mm-dd') + 1)";

        String sql = templeSql.replace("${date}", date);
        logger.info("FTP数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegContentFtp.class));
    }

    @Override
    public List<String[]> getFtpByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(100);
        return JdbcUtils.getDatabaseByPeriod(jdbcTemplate, tableName, startTime, endTime);
    }

    @Override
    public List<String> getFtpFieldValueByTime(String date) {

        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setResultsMapCaseInsensitive(true);
        jdbcTemplate.setFetchSize(100);
        String sql = "select * from REG_CONTENT_FTP where import_time >= to_date(? ,'yyyy-mm-dd') and import_time < (to_date(? ,'yyyy-mm-dd') + 1) and rownum < 2";

        return jdbcTemplate.query(sql, rs -> {
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
        });
    }

    @Override
    public RegContentFtp getFtpById(int id) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);

        String sql = "select * from REG_CONTENT_FTP where id = ?";


        List<RegContentFtp> list = jdbcTemplate.query(sql, new Object[]{id}, new BeanPropertyRowMapper<>(RegContentFtp.class));

        return list.get(0);
    }
}
