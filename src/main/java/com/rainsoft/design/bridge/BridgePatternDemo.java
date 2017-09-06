package com.rainsoft.design.bridge;

/**
 * 桥接模式将定义与其实现分离，它是一种结构模式。
 * 桥接(Bridge)模式涉及充当格拉的接口。格拉使得具体类与接口实现者类无关
 * 这两种类型的类可以改变但不会影响对方。
 * 当需要将抽象与其实现去耦合时使用桥接解耦(分离),使得两者可以独立地变化
 * 。这种类型的高度模式属于结构模式，此模式通过经它们之间提供桥接结构来将
 * 实现类类与抽象类解耦（分离）
 * 这种模式涉及一个接口作为一个桥梁，使得具体类的功能独立于接口实现类。
 * 两种类型的类可以在结构上改变而不彼此影响
 * Created by Administrator on 2017-09-05.
 */
public class BridgePatternDemo {
    public static void main(String[] args) {
        Shape redCircle = new Circle(100, 100, 10, new RedCircle());
        Shape greenCircle = new Circle(100, 100, 10, new GreenCircle());

        redCircle.draw();
        greenCircle.draw();
    }
}
