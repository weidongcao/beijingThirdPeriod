package com.rainsoft.dao;

import com.rainsoft.domain.RegContentFtp;
import com.rainsoft.inter.ContentDaoBaseInter;

import java.util.List;

/**
 * Created by Administrator on 2017-06-12.
 */
public interface FtpDao extends ContentDaoBaseInter {
    List<RegContentFtp> getFtpBydate(String date);
    List<String[]> getFtpByHours(String startTime, String endTime);

    List<String> getFtpFieldValueByTime(String date);

    RegContentFtp getFtpById(int id);
}
