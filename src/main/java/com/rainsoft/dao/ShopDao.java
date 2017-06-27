package com.rainsoft.dao;

import com.rainsoft.domain.RegContentShop;

import java.util.List;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public interface ShopDao {
    public List<RegContentShop> getShopByPeriod(String date);
}
