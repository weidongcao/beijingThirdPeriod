package com.rainsoft.utils;

import java.text.ParseException;
import java.util.Date;

/**
 * 日期工具类
 * Created by CaoWeiDong on 2017-10-19.
 */
public class DateUtils extends org.apache.commons.lang3.time.DateUtils {

    public static Date stringToDate(String date_String, String perseParttern) {
        Date date = null;
        try {
            date = parseDate(date_String, perseParttern);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return date;
    }

}
