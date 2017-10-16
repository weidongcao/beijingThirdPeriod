package com.rainsoft.lang3;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Administrator on 2017-10-16.
 */
public class TestDurationFormatUtils {
    protected static Date date1;
    protected static Date date2;

    static {
        try {
            date1 = DateUtils.parseDate("2017-10-16 13:30:00", "yyyy-MM-dd HH:mm:ss");
            date2 = DateUtils.parseDate("2017-10-16 13:00:00", "yyyy-MM-dd HH:mm:ss");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException, ParseException {
        String pattern = "yyyy-MM-dd HH:mm:ss";
        long durationMillis = (10 + 20 * 60 + 13 * 3600 + 4 * 24 * 3600) * 1000;
        String formatDate = DurationFormatUtils.formatDuration(durationMillis, pattern);
        System.out.println(formatDate);

        String[] parsePatterns = {"yyyy-MM-dd HH:mm:ss"};
        String str = "2009-09-29 15:30:12";
        String str2 = "2010-10-30 15:40:18";
        Date date = DateUtils.parseDate(str, parsePatterns);
        Date date2 = DateUtils.parseDate(str2, parsePatterns);
        durationMillis = DateUtils.getFragmentInMilliseconds(date, Calendar.MONTH);
        long durationMillis2 = DateUtils.getFragmentInMilliseconds(date2, Calendar.MONTH);

        String s = DurationFormatUtils.formatPeriod(durationMillis, durationMillis2, "yyyy-MM-dd HH:mm:ss");
        System.out.println("s = " + s);
    }
}
