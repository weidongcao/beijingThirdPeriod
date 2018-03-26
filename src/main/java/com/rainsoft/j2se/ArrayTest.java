package com.rainsoft.j2se;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017-06-15.
 */
public class ArrayTest {
    public static void main(String[] args) {

        Map<String, String> map = new HashMap<>();
        map.put("aaa", "AAA");
        map.put("bbb", "BBB");
        map.put("ccc", "CCC");
        System.out.println(map.toString());
    }
}
