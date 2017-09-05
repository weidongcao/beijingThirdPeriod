package com.rainsoft.design.builder;

/**
 * Created by Administrator on 2017-08-31.
 */
public abstract class Burger implements Item {
    @Override
    public Packing packing() {
        return new Wrapper();
    }

    @Override
    public abstract float price();
}
