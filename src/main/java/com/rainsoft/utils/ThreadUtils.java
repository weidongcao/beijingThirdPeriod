package com.rainsoft.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 线程工具类
 * Created by Administrator on 2017-10-19.
 */
public class ThreadUtils {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtils.class);

    public static void programSleep(int seconds) {
        try {
            while (seconds > 0) {
                if (seconds > 5) {
                    logger.info("程序休眠, 剩余时间: {} 秒...", seconds);
                    Thread.sleep(5 * 1000);
                    seconds -= 5;
                } else {
                    logger.info("程序休眠 {} 秒...", seconds);
                    Thread.sleep(seconds * 1000);
                    seconds = 0;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        programSleep(30);
    }
}
