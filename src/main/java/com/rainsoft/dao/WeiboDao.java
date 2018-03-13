package com.rainsoft.dao;

import com.google.common.base.Optional;
import com.rainsoft.domain.RegContentWeibo;

import java.util.List;

/**
 * 微博数据Dao层
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface WeiboDao {
    List<RegContentWeibo> getWeiboByPeriod(String date);

    List<String[]> getWeiboByHours(String startTime, String endTime);

    Optional<Long> getMinIdFromDate(Optional<String> date);

    List<String[]> getDataById(Optional<Long> id);
}
