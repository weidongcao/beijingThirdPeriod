package com.codewars.java;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by CaoWeiDong on 2018-01-09.
 */
public class SequenceSum {
    public static String showSequence(int value) {
        if (value < 0) {
            return value + "<0";
        } else if (value == 0) {
            return value + "=0";
        } else {

            StringBuilder sb = new StringBuilder();
            int sum = 0;
            for (int i = 0; i <= value; i++) {
                sum += i;
                if (i == value) {
                    sb.append(i)
                            .append(" ")
                            .append("=")
                            .append(" ")
                            .append(sum);
                } else {
                    sb.append(i).append("+");
                }
            }
            return sb.toString();
        }
    }

    @Test
    public void testBasic() {
        assertEquals("0+1+2+3+4+5+6 = 21", SequenceSum.showSequence(6));
    }
}
