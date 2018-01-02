package com.rainsoft.spark;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Created by CaoWeiDong on 2017-12-18.
 */
public class MyComparator implements Comparator<List<String>>, Serializable{

    private static final long serialVersionUID = 970508495812666665L;

    @Override
    public int compare(List<String> list1, List<String> list2) {
        if (list1.size() > list2.size()) {
            return 1;
        } else if (list1.size() == list2.size()) {
            return 0;
        } else {
            return -1;
        }
    }
}
