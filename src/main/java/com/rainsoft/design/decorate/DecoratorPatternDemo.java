package com.rainsoft.design.decorate;

/**
 * 装饰器模式允许用户向现有对象添加新功能而不改变其结构。这种类型的设计模式属于结构模式
 * 因为此模式充当现有类的包装器。
 * 此模式创建一个装饰器类，它包装原始类并提供附加功能，保持类方法签名完整。
 * 我们通过以下救命展示装饰器模式的使用，其中我们将用一些颜色装饰开关而不改变开关类。
 *
 * 在这个实例中，将创建一个Shape接口和实现Shape接口的具体类。然后再创建一个抽象装饰器类
 * -ShapeDecorator，实现Shape接口并使用Shape对象作为其实例变量。
 * 这是的RedShapeDecorator是实现ShapeDecorator的具体类。
 * DecoratorPatternDemo这是一个演示类。将使用RedShapeDecorator来装饰Shape对象
 * Created by CaoWeiDong on 2017-11-14.
 */
public class DecoratorPatternDemo {
    public static void main(String[] args) {
        Shape circle = new Circle();

        Shape redCircle = new RedShapeDecorator(new Circle());

        Shape redRectangle = new RedShapeDecorator(new Rectangle());
        System.out.println("Circle with normal border");

        circle.draw();

        System.out.println("\nCircle of red border");
        redCircle.draw();

        System.out.println("\nRectangle of red border");
        redRectangle.draw();
    }
}
