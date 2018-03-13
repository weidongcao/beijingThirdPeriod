package com.rainsoft.dao;

import com.google.common.base.Optional;
import com.rainsoft.domain.ServiceInfo;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface ServiceDao {
    List<ServiceInfo> getServiceByPeriod(String date);

    List<String[]> getServiceByHours(String startTime, String endTime);

    Optional<Long> getMinId();

    List<String[]> getDataById(Optional<Long> id);
}
