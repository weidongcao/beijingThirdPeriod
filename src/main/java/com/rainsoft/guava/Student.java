package com.rainsoft.guava;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.util.Comparator;

/**
 * Created by CaoWeiDong on 2018-01-17.
 */
public class Student implements Comparable<Student> {

    public String name;
    public int age;
    public int score;

    public Student(String name, int age, int score) {
        this.name = name;
        this.age = age;
        this.score = score;
    }

    @Override
    public int compareTo(Student o) {
        return ComparisonChain.start()
                .compare(name, o.name)
                .compare(age, o.age)
                .compare(score, o.score, Ordering.natural().nullsLast())
                .result();
    }
}

class StudentComparator implements Comparator<Student> {
    @Override
    public int compare(Student o1, Student o2) {
        return ComparisonChain.start()
                .compare(o1.name, o2.name)
                .compare(o1.age, o2.age)
                .compare(o1.score, o2.score)
                .result();
    }
}
