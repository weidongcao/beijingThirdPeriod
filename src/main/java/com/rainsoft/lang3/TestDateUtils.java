package com.rainsoft.lang3;

import org.apache.commons.lang3.time.DateUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Administrator on 2017-10-16.
 */
public class TestDateUtils {
    public static final DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static String aa = "2017-10-10 00:00:00";
    private static String AA = "2017-10-11 00:00:00";
    private static Date aaDate;
    private static Date AADate;
    static {
        try {
            aaDate = timeFormat.parse(aa);
            AADate = timeFormat.parse(AA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ParseException {
        Date date = DateUtils.parseDate("2017-10-16 13:29:49", "yyyy-mm-dd hh:mm:ss");
        System.out.println(DateUtils.ceiling(date, Calendar.MINUTE));
        System.out.println(DateUtils.round(date, Calendar.MINUTE));
        System.out.println(DateUtils.truncate(date, Calendar.MINUTE));

    }

    public static void testDateTruncate() {
        Calendar cal = Calendar.getInstance();
        System.out.println(timeFormat.format(cal.getTime()));
        cal = DateUtils.truncate(cal, Calendar.HOUR);
        System.out.println(timeFormat.format(cal.getTime()));
    }
}
