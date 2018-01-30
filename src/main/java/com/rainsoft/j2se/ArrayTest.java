package com.rainsoft.j2se;

import com.rainsoft.utils.DateUtils;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by Administrator on 2017-06-15.
 */
public class ArrayTest {
    public static void main(String[] args) {
        Date date = DateUtils.stringToDate("2018-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        System.out.println(calendar.get(Calendar.DAY_OF_MONTH));
        System.out.println("(int) = " + (int) 'B');
        System.out.println("(int) = " + (int) 'b');
        String[] aaa = new String[]{"aa", "bb", "cc"};
        for (int i = 0; i < 5; i++) {
            System.out.println(aaa[i]);
        }
    }
}
