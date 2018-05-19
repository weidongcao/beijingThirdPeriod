package com.rainsoft.j2se;

import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Created by CaoWeiDong on 2018-04-18.
 */
public class CollectionTest {
    public static Random random = new Random();
    public static void main(String[] args) {
        int size = 100000000;
        StopWatch watch = new StopWatch();
        watch.start();
        List list = arrayListWriteTest(size);
        Long aa = watch.getTime();
        System.out.println("ArrayList写入时间: " + aa);

        aa = watch.getTime();
        Integer sum = arrayListReadTest(list);
        System.out.println("ArrayList读取时间: " + (watch.getTime() - aa));
        System.out.println("sum = " + sum);

        aa = watch.getTime();
        list = linkWriteTest(size);
        System.out.println("LinkedList写入时间： " + (watch.getTime() - aa));

        aa = watch.getTime();
        sum = linkReadTest(list);
        System.out.println("LinkedList读取时间: " + (watch.getTime() - aa));
        System.out.println("sum = " + sum);

        aa = watch.getTime();
        Integer[] arr = arrayWriteTest(size);
        System.out.println("数组写入时间: " + (watch.getTime() - aa));

        aa = watch.getTime();
        sum = arrayReadTest(arr);
        System.out.println("数组读取时间: " + (watch.getTime() - aa));
        System.out.println("sum = " + sum);
        watch.stop();
    }
    public static List<Integer> arrayListWriteTest(int size) {
        List<Integer> list = new ArrayList();
        for (int i = 0; i < size; i++) {
            list.add(random.nextInt(10));
        }
        return list;
    }

    public static Integer arrayListReadTest(List<Integer> list) {
        int sum = 0;
        for (Integer i : list) {
            sum += i;
        }
        return sum;
    }

    public static Integer[] arrayWriteTest(int size) {
        Integer[] arr = new Integer[size];
        for (int i = 0; i < size; i++) {
            arr[i] = random.nextInt(10);
        }
        return arr;
    }

    public static Integer arrayReadTest(Integer[] arr) {
        int sum = 0;
        for (Integer i : arr) {
            sum += i;
        }
        return sum;
    }

    public static List<Integer> linkWriteTest(int size) {
        List<Integer> list = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            list.add(random.nextInt(10));
        }
        return list;
    }

    public static Integer linkReadTest(List<Integer> list) {
        int sum = 0;
        for (Integer i : list) {
            sum += i;
        }
        return sum;
    }
}
