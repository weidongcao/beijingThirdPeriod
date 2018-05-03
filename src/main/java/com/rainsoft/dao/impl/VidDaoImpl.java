package com.rainsoft.dao.impl;

import com.rainsoft.dao.VidDao;
import com.rainsoft.domain.RegVidInfo;
import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class VidDaoImpl extends JdbcDaoSupport implements VidDao {
    private static final Logger logger = LoggerFactory.getLogger(VidDaoImpl.class);

    private static final String tableName = "reg_vid_info";

    @Override
    public List<RegVidInfo> getVidByPeriod(String date) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.setFetchSize(1000);
        String templeSql = "select * from reg_vid_info";

        String sql = templeSql.replace("${date}", date);
        logger.info("Vid数据获取一天数据的sql: {}", sql);

        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(RegVidInfo.class));
    }

    @Override
    public List<String[]> getVidByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        String templeSql = "select *  from ${tableName} where last_logintime >= to_date('${startTime}' ,'yyyy-mm-dd hh24:mi:ss') and last_logintime < to_date('${endTime}' ,'yyyy-mm-dd hh24:mi:ss')";

        String sql = templeSql.replace("${startTime}", startTime)
                .replace("${endTime}", endTime)
                .replace("${tableName}", tableName);
        logger.info("{} 数据获取Oracle数据sql: {}", tableName, sql);

        /*
         * 返回结果为数组类型的List
         */
        return  jdbcTemplate.query(sql, JdbcUtils::resultSetToList);
    }
    /**
     * 根据日期获取在此日期之后最小(最早)的ID
     */
    @Override
    public Optional<Long> getMinId() {
        return JdbcUtils.getMinIdFromDate(getJdbcTemplate(), tableName, Optional.empty());
    }

    @Override
    public List<String[]> getDataByTime(String startTime, String endTime) {
        String sql = JdbcUtils.distinctTableTemplate.replace("${tableName}", tableName)
                .replace("${distinctTempTable}", "realid_info_inc")
                .replace("${dateField}", "update_time")
                .replace("${joinField}", "account")
                .replace("${startTime}", startTime)
                .replace("${endTime}", endTime);
        //两个表进行关联的时候是根据两个字段进行的，模板里的条件不够，需要再追加
        sql += "        AND B.${joinField} = A.${joinField}\n";
        sql = sql.replace("${joinField}", "protocol_type");

        //sql模板里面一层的sql是没有封装的，这样可以再添加查询条件
        sql += ")\n";
        return JdbcUtils.getDataBySql(getJdbcTemplate(), sql, "real");    }

    @Override
    public Optional<Date> getMinTime() {
        return JdbcUtils.getMinTime(getJdbcTemplate(), tableName, "update_time");
    }

    @Override
    public void delDataByTime(String startTime, String endTime) {

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
