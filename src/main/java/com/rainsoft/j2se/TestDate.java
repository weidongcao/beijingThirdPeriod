package com.rainsoft.j2se;

import com.rainsoft.utils.DateFormatUtils;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by CaoWeiDong on 2017-06-26.
 */
public class TestDate {
    public static void main(String[] args) throws ParseException {
        Calendar cal = Calendar.getInstance();
        System.out.println(DateFormatUtils.DATE_TIME_FORMAT.format(cal.getTime()));

        cal.set(Calendar.SECOND, 0);
        System.out.println(DateFormatUtils.DATE_TIME_FORMAT.format(cal.getTime()));
    }

    public static void getMS() throws ParseException {
        String aa = "2017-10-10 00:00:00";
        String AA = "2017-10-11 00:00:00";
        String bb = "2017-07-29";
        String BB = "2017-07-28";
        Date aaDate = DateFormatUtils.DATE_TIME_FORMAT.parse(aa);
        Date AADate = DateFormatUtils.DATE_TIME_FORMAT.parse(AA);
        Date bbDate = DateFormatUtils.ISO_DATE_FORMAT.parse(bb);
        Date BBDate = DateFormatUtils.ISO_DATE_FORMAT.parse(BB);

        Date sss = new Date(23906857445000l);
        System.out.println(DateFormatUtils.DATE_TIME_FORMAT.format(sss));
        System.out.println(aa + " =" + aaDate.getTime());
        System.out.println(AA + " =" + AADate.getTime());
        System.out.println(bb + " = " + bbDate.getTime());
        System.out.println(BB + " = " + BBDate.getTime());
        System.out.println(bbDate.getTime() - BBDate.getTime());

    }
}
