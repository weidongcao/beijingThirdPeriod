package com.rainsoft.dao;

import com.rainsoft.domain.RegContentBbs;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface BbsDao {
    public List<RegContentBbs> getBbsByPeriod(String date);
}
