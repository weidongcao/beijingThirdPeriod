package com.rainsoft.dao;

import com.rainsoft.domain.RegContentSearch;

import java.util.List;

/**
 * 搜索数据Dao接口
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface SearchDao {
    List<RegContentSearch> getSearchByPeriod(String date);

    List<String[]> getSearchByHours(String startTime, String endTime);
}
