package com.rainsoft.dao;

import com.rainsoft.domain.ServiceInfo;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface ServiceDao {
    List<ServiceInfo> getServiceByPeriod(String date);

    List<String[]> getServiceByHours(String startTime, String endTime);

    Long getMinId();

    List<String[]> getDataById(Long id);
}
