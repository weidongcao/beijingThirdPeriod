package com.rainsoft.dao;

import com.google.common.base.Optional;
import com.rainsoft.domain.RegContentShop;

import java.util.List;

/**
 * 购物数据Dao层
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface ShopDao {
    List<RegContentShop> getShopByPeriod(String date);

    List<String[]> getShopByHours(String startTime, String endTime);

    Long getMinIdFromDate(Optional<String> date);

    List<String[]> getDataById(Long id);
}
