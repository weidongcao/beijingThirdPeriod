package com.rainsoft.dao;

import com.rainsoft.domain.ServiceInfo;
import com.rainsoft.inter.InfoDaoInter;

import java.util.List;

/**
 * 场所表Dao层接口
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface ServiceDao extends InfoDaoInter {
    List<ServiceInfo> getServiceByPeriod(String date);

    List<String[]> getServiceByHours(String startTime, String endTime);
}
