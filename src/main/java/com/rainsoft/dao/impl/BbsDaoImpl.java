package com.rainsoft.dao.impl;

import com.rainsoft.dao.BbsDao;
import com.rainsoft.utils.JdbcUtils;
import com.rainsoft.utils.NamingRuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;
import java.util.Optional;

/**
 * Bbs论坛Dao层
 * Created by CaoWeiDong on 2017-06-28.
 */
public class BbsDaoImpl extends JdbcDaoSupport implements BbsDao {

    private static final Logger logger = LoggerFactory.getLogger(BbsDaoImpl.class);

    //BBS数据在Oracle中的表名
    private static final String tableName = NamingRuleUtils.getOracleContentTableName("bbs");

    /**
     * 查询指定开始时间和结束时间之间的数据并返回列表
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 查询返回结果
     */
    @Override
    public List<String[]> getBbsByHours(String startTime, String endTime) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        return JdbcUtils.getDatabaseByPeriod(jdbcTemplate, tableName, startTime, endTime);
    }

    /**
     * 根据日期获取在此日期之后最小(最早)的ID
     * @param optional 对日期字符串进行了封装的Optional<String>
     */
    @Override
    public Optional<Long> getMinIdFromDate(Optional<String> optional) {
        return JdbcUtils.getMinIdFromDate(getJdbcTemplate(), tableName, optional);
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
