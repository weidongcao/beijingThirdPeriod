package com.rainsoft.design.factory;

/**
 * Created by Administrator on 2017-08-30.
 */
public class FactoryPatternDemo {
    public static void main(String[] args) {
        ShapeFactory factory = new ShapeFactory();

        //get an object of Circle and call its draw method.
        Shape circle = factory.getShape("circle");

        //call draw method of Circle
        circle.draw();

        //get an object of Rectangle and call its draw method.
        Shape rectangle = factory.getShape("rectangle");

        //call draw method of Rectangle
        rectangle.draw();

        //get an object of Square and call its draw method.
        Shape square = factory.getShape("square");

        //call draw method of circle
        square.draw();
    }
}
