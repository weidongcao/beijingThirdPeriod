package com.rainsoft.j2se;

import com.rainsoft.utils.DateUtils;

import java.text.ParseException;
import java.util.Date;

/**
 * Created by CaoWeiDong on 2017-06-26.
 */
public class TestDate {
    public static void main(String[] args) throws ParseException {
        String aa = "2017-05-30";
        String AA = "2017-05-31";
        String bb = "2017-06-01";
        String BB = "2017-06-02";
        Date aaDate = DateUtils.DATE_FORMAT.parse(aa);
        Date AADate = DateUtils.DATE_FORMAT.parse(AA);
        Date bbDate = DateUtils.DATE_FORMAT.parse(bb);
        Date BBDate = DateUtils.DATE_FORMAT.parse(BB);
        System.out.println(aa + " =" + aaDate.getTime());
        System.out.println(AA + " =" + AADate.getTime());
        System.out.println(bb + " = " + bbDate.getTime());
        System.out.println(BB + " = " + BBDate.getTime());
    }
}
