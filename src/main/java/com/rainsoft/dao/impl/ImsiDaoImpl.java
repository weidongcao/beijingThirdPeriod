package com.rainsoft.dao.impl;

import com.rainsoft.dao.ImsiDao;
import com.rainsoft.utils.JdbcUtils;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Created by CaoWeiDong on 2018-04-24.
 */
public class ImsiDaoImpl extends JdbcDaoSupport implements ImsiDao{
    private static final String tableName = "scan_imsi_info";
    @Override
    public Optional<Long> getMinId() {
        return Optional.empty();
    }

    @Override
    public List<String[]> getDataByTime(String startTime, String endTime) {
        String sqlTemplate = "SELECT *\n" +
                "  FROM scan_imsi_info A\n" +
                " WHERE EXISTS\n" +
                "           (SELECT 1\n" +
                "              FROM imsi_info_inc B\n" +
                "             WHERE     A.imsi_code = b.imsi_code\n" +
                "                   AND A.sn_code = B.sn_code\n" +
                "                   AND B.UPDATE_TIME >= to_date('${start_time}', 'yyyy-mm-dd hh24:mi:ss')\n" +
                "                   AND B.UPDATE_TIME < to_date('${end_time}', 'yyyy-mm-dd hh24:mi:ss'))";
        String sql = sqlTemplate.replace("${start_time}", startTime)
                .replace("${end_time}", endTime);
        return JdbcUtils.getDataBySql(getJdbcTemplate(), sql, "imsi");
    }

    @Override
    public Optional<Date> getMinTime() {
        return JdbcUtils.getMinTime(getJdbcTemplate(), "imsi_info_inc", "update_time");
    }

    @Override
    public void delDataByTime(String startTime, String endTime) {
        JdbcUtils.delDataByTime(getJdbcTemplate(), "imsi_info_inc", "update_time", startTime, endTime);
    }

    @Override
    public List<String[]> getDatasByStartIDWithStep(Optional<Long> id) {
        return null;
    }
}
