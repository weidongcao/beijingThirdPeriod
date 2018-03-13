package com.rainsoft.dao;

import com.google.common.base.Optional;
import com.rainsoft.domain.RegVidInfo;

import java.util.List;

/**
 * Oralcle虚拟信息Dao层
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface VidDao {
    List<RegVidInfo> getVidByPeriod(String date);

    List<String[]> getVidByHours(String startTime, String endTime);

    Optional<Long> getMinId();

    List<String[]> getDataById(Optional<Long> id);
}
