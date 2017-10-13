package com.rainsoft.dao;

import com.rainsoft.domain.RegContentEmail;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface EmailDao {
    List<RegContentEmail> getEmailByPeriod(String date);

    List<String[]> getEmailByHours(String startTime, String endTime);
}
