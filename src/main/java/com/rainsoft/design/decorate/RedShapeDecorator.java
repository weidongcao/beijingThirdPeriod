package com.rainsoft.design.decorate;

/**
 *
 * Created by CaoWeiDong on 2017-11-14.
 */
class RedShapeDecorator extends ShapeDecorator {
    RedShapeDecorator(Shape decoratedShape) {
        super(decoratedShape);
    }

    @Override
    public void draw() {
        decoratedShape.draw();

        setRedBorder(decoratedShape);
    }

    private void setRedBorder(Shape decoratedShape) {
        System.out.println("Border Color: Red");
    }
}
