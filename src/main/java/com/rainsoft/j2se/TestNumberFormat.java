package com.rainsoft.j2se;

import java.text.NumberFormat;

/**
 * Created by CaoWeiDong on 2017-06-26.
 */
public class TestNumberFormat {
    public static void main(String[] args) {
        NumberFormat numberFormat = NumberFormat.getNumberInstance();
        Double myNumber=23323.3323232323;
        int aaa = 2374828;
        Double test=0.3434;
        System.out.println(numberFormat.format(myNumber));
        System.out.println(numberFormat.format(aaa));
    }
}
