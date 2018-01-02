package com.rainsoft.guava;


import com.google.common.base.Optional;

/**
 * Created by Administrator on 2017-08-29.
 */
public class TestGuava {

    public static void main(String[] args) {
        TestGuava tester = new TestGuava();
        Integer v1 = null;
        Integer v2 = new Integer(10);

        Optional<Integer> a = Optional.fromNullable(v1);
        Optional<Integer> b = Optional.of(v2);
        System.out.println("a.absent() = " + Optional.absent());
        System.out.println("b.absent() = " + Optional.absent());
        System.out.println("a.equals(b) = " + b.equals(Optional.of(Integer.valueOf(10))));
        System.out.println("b.hashCode() = " + b.hashCode());
        System.out.println(tester.sun(a, b));
    }

    public Integer sun(Optional<Integer> a, Optional<Integer> b) {
        System.out.println("First parameter is present: " + a.isPresent());
        System.out.println("Second parameter is present: " + b.isPresent());

        Integer v1 = a.or(new Integer(0));
        Integer v2 = b.get();

        return v1 + v2;
    }
}
