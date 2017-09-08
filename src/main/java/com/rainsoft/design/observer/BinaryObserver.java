package com.rainsoft.design.observer;

/**
 * Created by Administrator on 2017-09-07.
 */
public class BinaryObserver extends Observer {
    public BinaryObserver(Subject subject) {
        this.subject = subject;
        this.subject.attach(this);
    }

    @Override
    public void update() {
        System.out.println("Binary Stering: " + Integer.toBinaryString(subject.getState()));
    }
}
