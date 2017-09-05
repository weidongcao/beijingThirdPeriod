package com.rainsoft.design.builder;

/**
 * Created by Administrator on 2017-08-31.
 */
public class Pepsi extends ColdDrink  {
    @Override
    public String name() {
        return "Pepsi";
    }

    @Override
    public float price() {
        return 35.0f;
    }
}
