package com.rainsoft.design.prototype;

/**
 * Created by Administrator on 2017-09-01.
 */
public class Circle extends Shape {
    public Circle() {
        type = "Circle";
    }

    @Override
    void draw() {
        System.out.println("Inside Circle::draw() method.");
    }
}
