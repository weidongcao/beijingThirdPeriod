package com.rainsoft.design.abstractfactory;

import com.rainsoft.design.factory.Circle;
import com.rainsoft.design.factory.Rectangle;
import com.rainsoft.design.factory.Shape;
import com.rainsoft.design.factory.Square;

/**
 * Created by Administrator on 2017-08-30.
 */
public class ShapeFactory extends AbstractFactory {
    @Override
    Shape getShape(String shapeType) {
        if (null == shapeType) {
            return null;
        }

        if ("circle".equalsIgnoreCase(shapeType)) {
            return new Circle();
        } else if ("rectangle".equalsIgnoreCase(shapeType)) {
            return new Rectangle();
        } else if ("square".equalsIgnoreCase(shapeType)) {
            return new Square();
        }

        return null;
    }

    @Override
    Color getColor(String color) {
        return null;
    }


}
