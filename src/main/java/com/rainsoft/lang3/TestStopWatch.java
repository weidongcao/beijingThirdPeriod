package com.rainsoft.lang3;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 测试Commons.lang3的秒表类
 * 主要方法：
 * start()  --> 开始计时
 * split()  --> 设置Split点
 * getSplitTime()   --> 获取从start到最后一次split的时间
 * reset()  -->重置计时
 * suspend()    -->暂停计时,直到调用resume()后才恢复计时
 * stop()   --> 停止计时
 * getTime()    --> 统计从Start到现在的时间
 * Created by Administrator on 2017-10-16.
 *
 */
public class TestStopWatch {
    private static final Logger logger = LoggerFactory.getLogger(TestStopWatch.class);

    public static void main(String[] args) throws InterruptedException {
        StopWatch watch = new StopWatch();
        watch.start();
        logger.info("开始时间: {}", DurationFormatUtils.formatDuration(watch.getStartTime(), "yyyy-MM-dd HH:mm:ss"));

        Thread.sleep(3000);
        watch.split();
        logger.info("split time:{}", watch.getSplitTime());

        watch.reset();
        watch.start();

        watch.suspend();
        Thread.sleep(3000);
        watch.resume();

        Thread.sleep(3000);
        logger.info("再次开始时间:{}", DurationFormatUtils.formatDuration(watch.getTime(), "yyyy-MM-dd HH:mm:ss"));
        watch.stop();
        logger.info("结束时间:{}", DurationFormatUtils.formatDuration(watch.getTime(), "yyyy-MM-dd HH:mm:ss"));

    }

}
