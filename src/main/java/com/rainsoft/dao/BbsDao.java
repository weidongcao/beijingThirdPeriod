package com.rainsoft.dao;

import com.rainsoft.inter.ContentDaoInter;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface BbsDao extends ContentDaoInter {

    List<String[]> getBbsByHours(String startTime, String endTime);
}
