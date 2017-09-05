package com.rainsoft.design.factory;

/**
 * Created by Administrator on 2017-08-30.
 */
public class ShapeFactory {
    //use getShape method to get object of type shape
    public Shape getShape(String shapeType) {
        if (null == shapeType) {
            return null;
        } else if ("Circle".equalsIgnoreCase(shapeType)) {
            return new Circle();
        } else if ("Rectangle".equalsIgnoreCase(shapeType)) {
            return new Rectangle();
        } else if ("Square".equalsIgnoreCase(shapeType)) {
            return new Square();
        }

        return null;
    }
}
