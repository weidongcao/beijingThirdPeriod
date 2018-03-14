package com.rainsoft.dao;

import com.rainsoft.domain.RegContentWeibo;
import com.rainsoft.inter.ContentDaoBaseInter;

import java.util.List;

/**
 * 微博数据Dao层
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface WeiboDao extends ContentDaoBaseInter {
    List<RegContentWeibo> getWeiboByPeriod(String date);

    List<String[]> getWeiboByHours(String startTime, String endTime);
}
