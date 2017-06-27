package com.rainsoft.dao;

import com.rainsoft.domain.RegVidInfo;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface VidDao {
    public List<RegVidInfo> getVidByPeriod(String date);
}
