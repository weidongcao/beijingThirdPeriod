package com.rainsoft.dao;

import com.rainsoft.domain.RegContentWeibo;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface WeiboDao {
    public List<RegContentWeibo> getWeiboByPeriod(String date);
}
