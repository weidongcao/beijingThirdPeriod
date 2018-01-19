package com.codewars.java;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by CaoWeiDong on 2018-01-09.
 */
public class WhichAreIn {
    public static String[] inArray(String[] array1, String[] array2) {
        return Arrays.stream(array1)
                .filter(str -> Arrays.stream(array2).anyMatch(s -> s.contains(str)))
                .distinct()
                .sorted()
                .toArray(String[] :: new);
    }
    public static String[] myInArray(String[] array1, String[] array2) {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < array1.length; i++) {
            for (int j = 0; j < array2.length; j++) {
                if ((array2[j].contains(array1[i])) && (list.contains(array1[i]) == false)) {
                    list.add(array1[i]);
                    break;
                }
            }
        }
        Collections.sort(list);
        String[] arr = new String[list.size()];
        list.toArray(arr);
        return arr;
    }

    @Test
    public void test1() {
        String a[] = new String[]{"arp", "live", "strong"};
        String b[] = new String[]{"lively", "alive", "harp", "sharp", "armstrong"};
        String r[] = new String[]{"arp", "live", "strong"};
        assertArrayEquals(r, WhichAreIn.inArray(a, b));
    }
}
