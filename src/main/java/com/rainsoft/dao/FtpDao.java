package com.rainsoft.dao;

import com.google.common.base.Optional;
import com.rainsoft.domain.RegContentFtp;

import java.util.List;

/**
 * Created by Administrator on 2017-06-12.
 */
public interface FtpDao {
    List<RegContentFtp> getFtpBydate(String date);
    List<String[]> getFtpByHours(String startTime, String endTime);

    List<String> getFtpFieldValueByTime(String date);

    RegContentFtp getFtpById(int id);

    Optional<Long> getMinIdFromDate(Optional<String> date);

    List<String[]> getDataById(Optional<Long> id);
}
