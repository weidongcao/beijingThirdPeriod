package com.rainsoft.design.singleton;

/**
 * Created by Administrator on 2017-08-30.
 */
public class SingleObject {
    //create an object of SingleObejct
    private static SingleObject instance = new SingleObject();

    //make the constructor private so that this class cannot be instantiated
    private SingleObject(){}

    //get the only object available
    public static SingleObject getInstance() {
        if (null == instance) {
            instance = new SingleObject();
        }
        return instance;
    }

    public void showMessage() {
        System.out.println("hello word");
    }
}
