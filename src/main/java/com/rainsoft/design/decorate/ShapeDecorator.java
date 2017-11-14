package com.rainsoft.design.decorate;

/**
 * 抽象装饰器类
 * Created by CaoWeiDong on 2017-11-14.
 */
abstract class ShapeDecorator implements Shape {
    Shape decoratedShape;

    ShapeDecorator(Shape decoratedShape) {
        this.decoratedShape = decoratedShape;
    }

    @Override
    public void draw() {
        decoratedShape.draw();
    }
}
