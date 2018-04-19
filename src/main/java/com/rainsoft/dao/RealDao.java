package com.rainsoft.dao;

import com.rainsoft.domain.RegRealIdInfo;
import com.rainsoft.inter.InfoDaoInter;

import java.util.List;

/**
 * 真实表Dao层接口
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface RealDao extends InfoDaoInter {
    List<RegRealIdInfo> getRealByPeriod(String date);
}
