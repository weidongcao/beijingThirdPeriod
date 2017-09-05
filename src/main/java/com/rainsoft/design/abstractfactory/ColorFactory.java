package com.rainsoft.design.abstractfactory;

import com.rainsoft.design.factory.Shape;

/**
 * Created by Administrator on 2017-08-30.
 */
public class ColorFactory extends AbstractFactory {

    @Override
    Color getColor(String color) {
        if (null == color) {
            return null;
        }

        if ("red".equalsIgnoreCase(color)) {
            return new Red();
        } else if ("green".equalsIgnoreCase(color)) {
            return new Green();
        } else if ("blue".equalsIgnoreCase(color)) {
            return new Blue();
        }

        return null;
    }

    @Override
    Shape getShape(String shape) {
        return null;
    }
}
