package com.rainsoft.j2se;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hive.ql.stats.jdbc.JDBCStatsPublisher;
import org.junit.Test;

import java.util.Random;

/**
 * Created by CaoWeiDong on 2018-05-03.
 */
public class Passwd {
    public void createPasswd() {
        Random rnd = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 11; i++) {
            int ascii = 32 + 1 + rnd.nextInt(95);
            char c = (char) ascii;
            sb.append(c);
        }
        System.out.println("sb = " + sb.toString());

    }

    @Test
    public void createPasswdMiddle() {
        System.out.println(RandomStringUtils.randomAlphanumeric(11));
    }
}
