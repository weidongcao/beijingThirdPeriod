package com.rainsoft.dao;

import com.rainsoft.domain.RegContentShop;
import com.rainsoft.inter.ContentDaoInter;

import java.util.List;

/**
 * 购物数据Dao层
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface ShopDao extends ContentDaoInter {
    List<RegContentShop> getShopByPeriod(String date);

    List<String[]> getShopByHours(String startTime, String endTime);
}
