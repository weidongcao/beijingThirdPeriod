package com.rainsoft.lang3;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * Created by Administrator on 2017-10-17.
 */
public class TestRandom {
    public static void main(String[] args) {

        //生成一个9位的任意字符
        System.out.println("RandomStringUtils.random(9) = " + RandomStringUtils.random(9));
        //生成一个9位的英文和数字混合字符串
        System.out.println("RandomStringUtils.random(9, true, true) = " + RandomStringUtils.random(9, true, true));
        //生成一个9位的坠机数字
        System.out.println("RandomStringUtils.random(9, false, true) = " + RandomStringUtils.random(9, false, true));
        //生成一个9位的随机英文字符
        System.out.println("RandomStringUtils.random(9, true, false) = " + RandomStringUtils.random(9, true, false));
        //生成一个9位的随机数字
        System.out.println("RandomStringUtils.randomNumeric(9) = " + RandomStringUtils.randomNumeric(9));
        //生成一个9位的随机字母
        System.out.println("RandomStringUtils.randomAlphabetic(9) = " + RandomStringUtils.randomAlphabetic(9));
        //生成ggwh9wug的随机字母与数字
        System.out.println("RandomStringUtils.randomAlphanumeric(9) = " + RandomStringUtils.randomAlphanumeric(9));
    }

}
