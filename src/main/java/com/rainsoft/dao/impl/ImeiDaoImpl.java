package com.rainsoft.dao.impl;

import com.rainsoft.dao.ImeiDao;
import com.rainsoft.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Created by CaoWeiDong on 2018-04-24.
 */
public class ImeiDaoImpl extends JdbcDaoSupport implements ImeiDao{
    private static final Logger logger = LoggerFactory.getLogger(ImeiDaoImpl.class);
    private static final String tableName = "imei_info";
    @Override
    public Optional<Long> getMinId() {
        return Optional.empty();
    }

    @Override
    public List<String[]> getDataByTime(String startTime, String endTime) {
        return JdbcUtils.getDataByTime(getJdbcTemplate(), tableName, "update_time", startTime, endTime);
    }

    @Override
    public Optional<Date> getMinTime() {
        return JdbcUtils.getMinTime(getJdbcTemplate(), tableName, "update_time");
    }

    @Override
    public void delDataByTime(String startTime, String endTime) {

    }

    @Override
    public List<String[]> getDatasByStartIDWithStep(Optional<Long> id) {
        return null;
    }
}
