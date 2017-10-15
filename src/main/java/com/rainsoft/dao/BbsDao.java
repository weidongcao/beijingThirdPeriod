package com.rainsoft.dao;

import com.rainsoft.domain.RegContentBbs;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface BbsDao {
    List<RegContentBbs> getBbsByPeriod(String date);

    List<String[]> getBbsByHours(String startTime, String endTime);
}
