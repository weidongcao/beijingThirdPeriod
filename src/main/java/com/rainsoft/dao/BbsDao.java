package com.rainsoft.dao;

import com.google.common.base.Optional;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface BbsDao {

    List<String[]> getBbsByHours(String startTime, String endTime);

    Optional<Long> getMinIdFromDate(Optional<String> date);

    List<String[]> getDataById(Optional<Long> id);
}
