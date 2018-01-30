package com.rainsoft.j2se;

import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created by CaoWeiDong on 2018-01-10.
 */
public class CreateRandomData {
    private static final DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final Random rand = new Random();
    private static Date beginStartTime = null;
    private static Date beginEndTime = null;
    private static Date endStartTime = null;
    private static Date endEndTime = null;
    public static List<String> macs = new ArrayList<>();
    private static List<String> services = new ArrayList<>();
    private static final String regex = "(.{2})";

    static {
        for (int i = 0; i < 10000; i++) {
            String ss = RandomStringUtils.randomAlphanumeric(12);
            ss = ss.replaceAll(regex, "$1-");
            ss = ss.substring(0, ss.length() - 1);
            macs.add(ss.toUpperCase());
        }
        for (int i = 0; i < 100; i++) {
            services.add("310107299" + (rand.nextInt(10000) + 10000));
        }
        try {
            beginStartTime = dateFormat.parse("2018-01-01");
            beginEndTime = dateFormat.parse("2018-01-03");
            endStartTime = dateFormat.parse("2018-01-08");
            endEndTime = dateFormat.parse("2018-01-10");
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        int datacount = 0;
        int filecount = 1;
        File file;
        String path = "D:\\0WorkSpace\\data\\data\\Phoenix\\data";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100000000; i++) {
//        for (int i = 0; i < 100; i++) {
            sb.append(SolrUtil.createRowkey())
                    .append("\t")
                    .append(services.get(rand.nextInt(services.size())))
                    .append("\t")
                    .append(macs.get(rand.nextInt(macs.size())))
                    .append("\t")
                    .append(randomDate(beginStartTime, beginEndTime))
                    .append("\t")
                    .append(randomDate(endStartTime, endEndTime))
                    .append("\t")
                    .append(RandomStringUtils.randomAlphanumeric(20))
                    .append("\r\n");
            datacount++;
            if (datacount >= 2000000) {
                file = FileUtils.getFile(path + filecount);
                FileUtils.writeStringToFile(file, sb.toString(), "utf-8", false);
                datacount = 0;
                filecount++;
                sb.delete(0, sb.length());
                sb = new StringBuilder();
            }

        }

    }

    /**
     * 获取随机日期
     *
     * @return
     */
    private static String randomDate(Date start, Date end) {
        try {
            if (start.getTime() >= end.getTime()) {
                return null;
            }
            long date = random(start.getTime(), end.getTime());
            return timeFormat.format(new Date(date));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static long random(long begin, long end) {
        long rtn = begin + (long) (Math.random() * (end - begin));
        // 如果返回的是开始时间和结束时间，则递归调用本函数查找随机值
        if (rtn == begin || rtn == end) {
            return random(begin, end);
        }
        return rtn;
    }
}
