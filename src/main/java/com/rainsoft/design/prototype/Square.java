package com.rainsoft.design.prototype;

/**
 * Created by Administrator on 2017-09-01.
 */
public class Square extends Shape {
    public Square() {
        type = "Square";
    }

    @Override
    void draw() {
        System.out.println("Inside Square::draw() method.");
    }
}
