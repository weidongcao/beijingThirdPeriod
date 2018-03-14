package com.rainsoft.dao;

import com.rainsoft.inter.ContentDaoBaseInter;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface BbsDao extends ContentDaoBaseInter {

    List<String[]> getBbsByHours(String startTime, String endTime);
}
