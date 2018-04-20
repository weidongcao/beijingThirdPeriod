package com.rainsoft.inter;

import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * ISec管理综合平台Dao层信息表的基类
 * 包括real, vid, service
 * Created by CaoWeiDong on 2018-03-14.
 */
public interface InfoDaoInter extends ISecDaoBaseInter {
    //获取数据库表中从指定日期开始最小的ID（数据库表里ID是自动递增的序列）
    Optional<Long> getMinId();

    //根据起始截止时间查询数据
    List<String[]> getDataByTime(String startTime, String endTime);

    //查询指定的日期字段最小的时间
    Optional<Date> getMinTime();
}
