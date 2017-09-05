package com.rainsoft.design.factory;

/**
 * Created by Administrator on 2017-08-30.
 */
public class Rectangle implements Shape {
    @Override
    public void draw() {
        System.out.println("Inside Rectangle::draw() method.");
    }
}
