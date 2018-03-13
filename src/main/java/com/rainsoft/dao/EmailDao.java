package com.rainsoft.dao;

import com.google.common.base.Optional;
import com.rainsoft.domain.RegContentEmail;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface EmailDao {
    List<RegContentEmail> getEmailByPeriod(String date);

    List<String[]> getEmailByHours(String startTime, String endTime);

    Optional<Long> getMinIdFromDate(Optional<String> date);

    List<String[]> getDataById(Optional<Long> id);
}
