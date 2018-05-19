package com.rainsoft.j2se;

/**
 * Created by CaoWeiDong on 2018-05-04.
 */
public class CountOperate extends Thread{
    public CountOperate() {
        super("Thread-CO");
        System.out.println("CountOperate---begin");
        System.out.println("Thread.currentThread().getName()=" + Thread.currentThread().getName());
        System.out.println("this.getName()=" + this.getName());
        System.out.println("CountOperate---end");
    }

    @Override
    public void run() {
        System.out.println("run---begin");
        System.out.println("Thread.currentThread().getName()=" + Thread.currentThread().getName());
        System.out.println("this.getName9)=" + this.getName());
        System.out.println("run---end");
    }

    public static void main(String[] args) {
        CountOperate c = new CountOperate();
        Thread t1 = new Thread(c);
        System.out.println(t1.getName() + ":ID --> " + t1.getId());
        t1.setName("A");
        t1.start();
        c.start();
    }
}
