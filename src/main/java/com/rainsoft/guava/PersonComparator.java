package com.rainsoft.guava;

import com.google.common.primitives.Ints;

import java.util.Comparator;

/**
 * Created by CaoWeiDong on 2018-01-17.
 */
public class PersonComparator implements Comparator<Persion> {
    @Override
    public int compare(Persion o1, Persion o2) {
        int result = o1.getName().compareTo(o2.getName());
        if (result != 0) {
            return result;
        }
        return Ints.compare(o1.getAge(), o1.getAge());
    }
}
