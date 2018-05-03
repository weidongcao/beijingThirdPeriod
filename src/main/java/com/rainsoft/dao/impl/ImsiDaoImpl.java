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
        String sql = JdbcUtils.distinctTableTemplate.replace("${tableName", tableName)
                .replace("${distinctTempTable}", "imsi_info_inc")
                .replace("${dateField}", "update_time")
                .replace("${joinField}", "imsi_code")
                .replace("${startTime}", startTime)
                .replace("${endTime}", endTime);

        //两个表进行关联的时候是根据两个字段进行的，模板里的条件不够，需要再追加
        sql += "        AND B.${joinField} = A.${joinField}\n";
        sql = sql.replace("${joinField}", "sn_code");

        //sql模板里面一层的sql是没有封装的，这样可以再添加查询条件
        sql += ")\n";

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
