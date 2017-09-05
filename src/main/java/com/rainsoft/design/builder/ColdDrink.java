package com.rainsoft.design.builder;

/**
 * Created by Administrator on 2017-08-31.
 */
public abstract class ColdDrink implements Item {
    @Override
    public Packing packing() {
        return new Bottle();
    }

    @Override
    public abstract float price();
}
