package com.rainsoft.j2se;

import com.rainsoft.utils.DateUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by CaoWeiDong on 2017-06-26.
 */
public class TestDate {
    public static void main(String[] args) throws ParseException {
        String aa = "2017-08-15 00:00:00";
        String AA = "2017-08-16 00:00:00";
        String bb = "2017-07-29";
        String BB = "2017-07-28";
        Date aaDate = DateUtils.TIME_FORMAT.parse(aa);
        Date AADate = DateUtils.TIME_FORMAT.parse(AA);
        Date bbDate = DateUtils.DATE_FORMAT.parse(bb);
        Date BBDate = DateUtils.DATE_FORMAT.parse(BB);

        Date sss = new Date(23906857445000l);
        System.out.println(DateUtils.TIME_FORMAT.format(sss));
        System.out.println(aa + " =" + aaDate.getTime());
        System.out.println(AA + " =" + AADate.getTime());
        System.out.println(bb + " = " + bbDate.getTime());
        System.out.println(BB + " = " + BBDate.getTime());
        System.out.println(bbDate.getTime() - BBDate.getTime());
    }
}
