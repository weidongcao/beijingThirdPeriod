package com.rainsoft.dao;

import com.rainsoft.domain.RegContentFtp;

import java.util.List;

/**
 * Created by Administrator on 2017-06-12.
 */
public interface FtpDao {
    public List<RegContentFtp> getFtpBydate(String date);

    public List<String> getFtpFieldValueByTime(String date);

    public RegContentFtp getFtpById(int id);
}
