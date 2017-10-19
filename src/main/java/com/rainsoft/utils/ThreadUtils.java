package com.rainsoft.utils;

/**
 *
 * 线程工具类
 * Created by Administrator on 2017-10-19.
 */
public class ThreadUtils {
    public static void programSleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
