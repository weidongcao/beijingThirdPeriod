package com.rainsoft.java8;

import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by Administrator on 2017-09-06.
 */
public class TestJava8 {
    public static void main(String[] args) {
        List<String> strs = Arrays.asList( "zzz", "bbb", "ccc", "ddd", "xxx", "yyy", "aaa", "", " ", "   ");
        List<String> filtered = strs.stream().filter(
                str -> !str.isEmpty()
        ).sorted()
        .collect(Collectors.toList());
//        filtered.stream().forEach(System.out::println);
        List<Integer> nums = Arrays.asList(29, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 1, 19, 20, 21, 22, 23, 24, 25, 26, 2, 27, 28, 30);
        nums.stream()
                .map(num -> num*num*num)
                .distinct()
                .sorted()
                .limit(20)
                .forEach(System.out::println);
        IntSummaryStatistics status = nums.stream().mapToInt((x) -> x).summaryStatistics();
        System.out.println("列表中最大的数: " + status.getMax());
        System.out.println("列表中最小的数: " + status.getMin());
        System.out.println("所有数之和: " + status.getSum());
        System.out.println("平均数: " + status.getAverage());
    }
}
