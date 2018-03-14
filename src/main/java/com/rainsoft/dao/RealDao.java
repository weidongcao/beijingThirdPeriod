package com.rainsoft.dao;

import com.rainsoft.domain.RegRealIdInfo;
import com.rainsoft.inter.InfoDaoBaseInter;

import java.util.List;

/**
 * 真实表Dao层接口
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface RealDao extends InfoDaoBaseInter{
    List<RegRealIdInfo> getRealByPeriod(String date);

    List<String[]> getRealByHours(String startTime, String endTime);
}
