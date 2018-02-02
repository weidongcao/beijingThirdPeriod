package com.rainsoft.guava;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import org.junit.Test;

import java.awt.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by CaoWeiDong on 2018-01-23.
 */
public class ImmutableTest {
    public void testJDKImmutable() {
        List<String> list = new ArrayList<>();
        list.add("aaa");
        list.add("bbb");
        list.add("ccc");
        System.out.println(list);

        List<String> unmodifiableList = Collections.unmodifiableList(list);
        System.out.println(unmodifiableList);

        List<String> unmodifiableList1 = Collections.unmodifiableList(Arrays.asList("aaa", "bbb", "ccc"));
        System.out.println(unmodifiableList1);

        String temp = unmodifiableList.get(1);
        System.out.println("unmodifiableList[0]: " + temp);

        list.add("body");
        System.out.println("list add a item after list: " + list);
        System.out.println("list add a item after unmodifiableList: " + unmodifiableList);

        unmodifiableList1.add("ddd");
        System.out.println("unmodifiableList add a item after list: " + unmodifiableList);

        unmodifiableList.add("eee");
        System.out.println("unmodifiableList add a item after list: " + unmodifiableList);
    }

    public void testGuavaImmutable() {
        List<String> list = new ArrayList<>();
        list.add("aaa");
        list.add("bbb");
        list.add("ccc");
        System.out.println("list: " + list);

        ImmutableList<String> imlist = ImmutableList.copyOf(list);
        System.out.println("imlist = " + imlist);

        ImmutableList<String> imOflist = ImmutableList.of("dong", "huan", "ling");
        System.out.println("imOflist = " + imOflist);

        ImmutableSortedSet<String> imSortList = ImmutableSortedSet.of("aaa", "bbb", "ccc", "ddd");
        System.out.println("imSortList = " + imSortList);

        list.add("baby");
        System.out.println("list add a item after list: " + list);
        System.out.println("list add a item after imlist: " + imlist);

        ImmutableSet<Color> imColorSet = ImmutableSet.<Color>builder()
                .add(new Color(0, 255, 255))
                .add(new Color(0, 191, 255))
                .build();
        System.out.println("imColorSet = " + imColorSet);
    }

    public void testCopyOf() {
        ImmutableSet<String> imSet = ImmutableSet.of("peida", "jerry", "harry", "lisa");
        System.out.println("imSet: " + imSet);
        ImmutableList<String> imlist = ImmutableList.copyOf(imSet);
        System.out.println("imlist = " + imlist);
        ImmutableSortedSet<String> imSortSet = ImmutableSortedSet.copyOf(imSet);
        System.out.println("imSortSet = " + imSortSet);

        List<String> list = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            list.add("bigdata-" + i);
        }

        System.out.println("list = " + list);
        ImmutableList<String> imInfolist = ImmutableList.copyOf(list.subList(2, 9));
        System.out.println("imInfolist = " + imInfolist);
        int imInfolistSize = imInfolist.size();
        System.out.println("imInfolistSize = " + imInfolistSize);
        ImmutableSet<String> imInfoSet = ImmutableSet.copyOf(imInfolist.subList(2, imInfolistSize - 3));
        System.out.println("imInfoSet = " + imInfoSet);
    }

    @Test
    public void testAsList() {
        ImmutableList<String> imList = ImmutableList.of("peida", "jerry", "harry", "lisa", "jerry");
        System.out.println("imList = " + imList);
        ImmutableSortedSet<String> imSortSet = ImmutableSortedSet.copyOf(imList);
        System.out.println("imSortSet = " + imSortSet);
        System.out.println("imSortSet.asList() = " + imSortSet.asList());
    }
}
