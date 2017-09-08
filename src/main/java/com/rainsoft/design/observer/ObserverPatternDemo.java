package com.rainsoft.design.observer;

/**
 * 观察者模式在对象之间存在一对多关系时使用。例如，如果一个对象被修改，
 * 它的依赖对象将被自动通知。它的依赖对象将被迫自动通知。
 * 观察者模式属于行为模式类别
 * Created by Administrator on 2017-09-07.
 */
public class ObserverPatternDemo {
    public static void main(String[] args) {
        Subject subject = new Subject();

        new HexaObserver(subject);
        new OctalObserver(subject);
        new BinaryObserver(subject);

        System.out.println("First State chage: 15");
        subject.setState(15);

        System.out.println("Second State Chage: 10");
        subject.setState(10);
    }
}
