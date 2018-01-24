package com.rainsoft.j2se;

import org.junit.Test;

import java.text.NumberFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by CaoWeiDong on 2017-06-26.
 */
public class TestNumberFormat {
    public void numberFormat() {
        NumberFormat numberFormat = NumberFormat.getNumberInstance();
        Double myNumber=23323.3323232323;
        int aaa = 2374828;
        Double test=0.3434;
        System.out.println(numberFormat.format(myNumber));
        System.out.println(numberFormat.format(aaa));
    }
    public static Long getCurrentTime(Long l) {
        //毫秒时间转成分钟
        double doubleTime = (Math.floor(l / 1000L));
        //往下取整 1.9=> 1.0
        long floorValue = new Double(doubleTime).longValue();
        return floorValue * 1000;
    }
    public static Long process(Long l) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(l);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTimeInMillis() / 1000;
    }
    public static Long getformatLong(Long l) {
        String lo = l.toString();
        if (lo.endsWith("000") == false) {
            return Long.valueOf(lo.substring(0, lo.length() - 3) + "000");
        } else {
            return Long.valueOf(lo);
        }
    }

    @Test
    public void test() {
        long l = new Date().getTime();
        System.out.println("l = " + l);
        System.out.println("l = " + ((l / 1000) * 1000));
    }
    public void compareLongTruncate() {
         int num = 10000000;
        for (int j = 0; j < 10; j++) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < num; i++) {
                getCurrentTime(start);
            }
            long end = System.currentTimeMillis();
            System.out.println("getCurrentTime第" + j + "次：" + (end - start));

            start = System.currentTimeMillis();
            for (int i = 0; i < num; i++) {
                process(start);
            }
            end = System.currentTimeMillis();
            System.out.println("process第" + j + "次：" + (end - start));

            start = System.currentTimeMillis();
            for (int i = 0; i < num; i++) {
                getformatLong(start);
            }
            end = System.currentTimeMillis();
            System.out.println("getFormatLong第" + j + "次：" + (end - start));
        }
    }
}
