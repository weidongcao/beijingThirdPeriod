package com.rainsoft.j2se;

/**
 * Created by Administrator on 2017-06-19.
 */
public class StringTest {
    //字段间分隔符
    private static String kvOutSeparator = "\\|;\\|";

    private static String kvInnerSeparator = "\\|=\\|";

    public static void main(String[] args) {
        String aaa = "aaa|=|AAA|;|bbb|=|BBB";
        String[] arr = aaa.split(kvOutSeparator);
        for (String str : arr) {
            String[] kv = str.split(kvInnerSeparator);
            System.out.println(kv[0] + "= " + kv[1]);
        }

    }
}
