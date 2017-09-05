package com.rainsoft.design.abstractfactory;

import com.rainsoft.design.factory.Shape;

/**
 * Created by Administrator on 2017-08-30.
 */
public class AbstractFactoryPatternDemo {
    public static void main(String[] args) {
        //get shape factory
        AbstractFactory shapeFactory = FactoryProducer.getFactory("shape");

        //get an object of shape circle
        Shape shape1 = shapeFactory.getShape("circle");

        //call draw method of Shape Circle
        shape1.draw();

        //get an object of Shape Rectangle
        Shape shape2 = shapeFactory.getShape("rectangle");

        //call draw method of Shape Rectangle
        shape2.draw();

        //get an object of Shape Square
        Shape shape3 = shapeFactory.getShape("square");

        //call draw method of Shape Square
        shape3.draw();

        //get color factory
        AbstractFactory colorFactory = FactoryProducer.getFactory("color");

        //get an object of Color Red
        Color color1 = colorFactory.getColor("red");

        //call an object of Color Red
        color1.fill();

        //get an object of Color Green
        Color color2 = colorFactory.getColor("green");

        //call fill method of Green
        color2.fill();

        //get an object of Color Blue
        Color color3 = colorFactory.getColor("blue");

        //call fill method of Color Blue
        color3.fill();
    }
}
