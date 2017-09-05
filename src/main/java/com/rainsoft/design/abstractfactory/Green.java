package com.rainsoft.design.abstractfactory;

/**
 * Created by Administrator on 2017-08-30.
 */
public class Green implements Color {

    @Override
    public void fill() {
        System.out.println("Inside Green::file() method.");
    }
}
