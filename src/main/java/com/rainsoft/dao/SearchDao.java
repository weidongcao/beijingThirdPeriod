package com.rainsoft.dao;

import com.rainsoft.domain.RegContentSearch;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface SearchDao {
    public List<RegContentSearch> getSearchByPeriod(String date);
}
