package com.rainsoft.guava;


/**
 * Created by CaoWeiDong on 2018-01-17.
 */
public class Persion implements Comparable<Persion> {
    private String name;
    private int age;

    public Persion(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public int compareTo(Persion o) {
        int cmpName = name.compareTo(o.name);
        if (cmpName != 0) {
            return cmpName;
        }
        if (age > o.age) {
            return 1;
        } else if (age < o.age) {
            return -1;
        }
        return 0;
    }
}
