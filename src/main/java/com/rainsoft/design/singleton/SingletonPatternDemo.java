package com.rainsoft.design.singleton;

/**
 * Created by Administrator on 2017-08-30.
 */
public class SingletonPatternDemo {
    public static void main(String[] args) {
        //illegal construct
        //compile time error: the constructor SingleObject() is not visible
        //SingleObject object = new SingleObject()

        //get the only object available
        SingleObject object = SingleObject.getInstance();

        //show the message
        object.showMessage();
    }
}
