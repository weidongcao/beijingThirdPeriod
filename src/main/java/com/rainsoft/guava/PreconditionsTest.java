package com.rainsoft.guava;

import com.google.common.base.Preconditions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by CaoWeiDong on 2018-01-16.
 */
public class PreconditionsTest {
    @Test
    public void Preconditions() throws Exception {
        getPersonByPrecondition(8, "dong");
        try {
            getPersonByPrecondition(-9, "dong");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        try {
            getPersonByPrecondition(8, "");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        try {
            getPersonByPrecondition(8, null);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        List<Integer> intList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            try {
                checkState(intList, 9);
                intList.add(i);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        try {
            checkPositionIndex(intList, 3);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        try {
            checkPositionIndex(intList, 13);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        try {
            checkPositionIndexs(intList, 3, 7);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        try {
            checkPositionIndexs(intList, 3, 17);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        try {
            checkPositionIndexs(intList, 13, 17);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        try {
            checkElementIndex(intList, 6);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        try {
            checkElementIndex(intList, 16);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void checkState(List<Integer> intList, int index) throws Exception {
        Preconditions.checkState(
                intList.size() < index,
                "intList size 不能大于" + index
        );
    }

    public static void checkPositionIndex(List<Integer> intList, int index) throws Exception {
        Preconditions.checkPositionIndex(
                index,
                intList.size(),
                "index " + index + " 不在list中, List size为： " + intList.size()
        );
    }

    public static void checkPositionIndexs(List<Integer> intList, int start, int end) throws Exception {
        Preconditions.checkPositionIndexes(start, end, intList.size());
    }

    public static void checkElementIndex(List<Integer> intList, int index) throws Exception {
        Preconditions.checkElementIndex(
                index,
                intList.size(),
                "index 为 " + index + " 不在List中, List Size为: " + intList.size());
    }

    private static void getPersonByPrecondition(int age, String name) throws Exception {
        Preconditions.checkNotNull(name, "name 为null");
        Preconditions.checkArgument(name.length() > 0, "name为\'\'");
        Preconditions.checkArgument(age > 0, "age 必须大于0");
        System.out.println("a person age: " + age + " ,name: " + name);
    }
}
