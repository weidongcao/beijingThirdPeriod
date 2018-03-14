package com.rainsoft.dao;

import com.rainsoft.domain.RegContentHttp;
import com.rainsoft.inter.ContentDaoBaseInter;

import java.util.List;

/**
 * Created by Administrator on 2017-06-12.
 */
public interface HttpDao extends ContentDaoBaseInter {
    List<RegContentHttp> getHttpBydate(String date, float startPercent, float endPercent);

    List<String[]> getHttpByHours(String startTime, String endTime);
}
