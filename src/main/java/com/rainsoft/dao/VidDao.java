package com.rainsoft.dao;

import com.rainsoft.domain.RegVidInfo;
import com.rainsoft.inter.InfoDaoBaseInter;

import java.util.List;

/**
 * Oralcle虚拟信息表Dao层
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface VidDao extends InfoDaoBaseInter{
    List<RegVidInfo> getVidByPeriod(String date);

    List<String[]> getVidByHours(String startTime, String endTime);
}
