package com.rainsoft.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * 线程工具类
 * Created by Administrator on 2017-10-19.
 */
public class ThreadUtils {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtils.class);

    public static void programSleep(int seconds) {
        try {
            for (int i = 0; i < seconds / 5; i++) {
                logger.info("程序休眠, 剩余时间: {} 秒...", seconds - 5 * i);
                Thread.sleep(5 * 1000);
            }
            if (seconds > 0) {
                logger.info("等待结束");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        programSleep(30);
    }
}
