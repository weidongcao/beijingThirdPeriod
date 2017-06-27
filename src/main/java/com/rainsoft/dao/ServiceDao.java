package com.rainsoft.dao;

import com.rainsoft.domain.ServiceInfo;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface ServiceDao {
    public List<ServiceInfo> getServiceByPeriod(String date);
}
