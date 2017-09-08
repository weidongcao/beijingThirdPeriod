package com.rainsoft.design.observer;

/**
 * Created by Administrator on 2017-09-07.
 */
public abstract class Observer {
    Subject subject;

    public abstract void update();
}
