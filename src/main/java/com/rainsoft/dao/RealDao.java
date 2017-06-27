package com.rainsoft.dao;

import com.rainsoft.domain.RegRealIdInfo;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface RealDao {
    public List<RegRealIdInfo> getRealByPeriod(String date);
}
