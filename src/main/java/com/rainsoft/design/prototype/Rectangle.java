package com.rainsoft.design.prototype;

/**
 * Created by Administrator on 2017-09-01.
 */
public class Rectangle extends Shape {
    public Rectangle() {
        type = "Rectangle";
    }

    @Override
    void draw() {
        System.out.println("Inside Rectangle::draw() method.");
    }
}
