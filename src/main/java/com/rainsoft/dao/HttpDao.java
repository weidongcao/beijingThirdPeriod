package com.rainsoft.dao;

import com.rainsoft.domain.RegContentHttp;

import java.util.List;

/**
 * Created by Administrator on 2017-06-12.
 */
public interface HttpDao {
    List<RegContentHttp> getHttpBydate(String date, float startPercent, float endPercent);
}
