package com.rainsoft.design.builder;

/**
 * Created by Administrator on 2017-08-31.
 */
public interface Item {

    /**
     * 食品名称
     * @return
     */
    String name();

    /**
     * 食品所在的包装
     * @return
     */
    Packing packing();

    /**
     * 食品的价格
     * @return
     */
    float price();
}
