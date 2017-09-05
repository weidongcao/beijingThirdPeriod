package com.rainsoft.design.abstractfactory;

import com.rainsoft.design.factory.Shape;

/**
 * Created by Administrator on 2017-08-30.
 */
public abstract class AbstractFactory {
    abstract Color getColor(String color);

    abstract Shape getShape(String shape);
}
