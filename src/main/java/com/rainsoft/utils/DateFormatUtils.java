package com.rainsoft.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 */
public class DateFormatUtils extends org.apache.commons.lang3.time.DateFormatUtils {
    public static final DateFormat DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final DateFormat HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH");

    //SOlr日期格式
    public static DateFormat SOLR_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public static final DateFormat NO_SEPARATOR_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

}
