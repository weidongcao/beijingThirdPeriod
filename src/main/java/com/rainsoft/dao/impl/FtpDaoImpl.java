package com.rainsoft.dao.impl;

import com.rainsoft.dao.FtpDao;
import com.rainsoft.domain.RegContentFtp;
import com.rainsoft.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
                    if (i < 22) {
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
