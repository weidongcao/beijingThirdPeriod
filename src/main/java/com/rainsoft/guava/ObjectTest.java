package com.rainsoft.guava;

import com.google.common.base.Objects;
import org.junit.Test;

/**
 * Created by CaoWeiDong on 2018-01-17.
 */
public class ObjectTest {
    public void equalTest() {
        System.out.println(Objects.equal("a", "a"));
        System.out.println(Objects.equal(null, "a"));
        System.out.println(Objects.equal("a", null));
        System.out.println(Objects.equal(null, null));
    }
    public void equalPersonTest() {
        System.out.println(Objects.equal(new Persion("dong", 28), new Persion("dong", 28)));
        Persion persion = new Persion("dong", 28);
        System.out.println(Objects.equal(persion, persion));
    }
    public void hashCodeTest() {
        System.out.println(Objects.hashCode("a"));
        System.out.println(Objects.hashCode("a"));
        System.out.println(Objects.hashCode("a", "b"));
        System.out.println(Objects.hashCode("b", "a"));
        System.out.println(Objects.hashCode("a", "b", "c"));

        Persion persion = new Persion("dong", 28);
        System.out.println(Objects.hashCode(persion));
        System.out.println(Objects.hashCode(persion));
    }
    public void toStringTest() {
        System.out.println(Objects.toStringHelper(this).add("x", 1).toString());
        System.out.println(Objects.toStringHelper(Persion.class).add("x", 1).toString());

        Persion persion = new Persion("dong", 28);
        String result = Objects.toStringHelper(Persion.class)
                .add("name", persion.getName())
                .add("age", persion.getAge()).toString();
        System.out.println(result);
    }
    @Test
    public void compareTest() {
        Persion p1 = new Persion("dong", 21);
        Persion p2 = new Persion("aaa", 27);
        Persion p3 = new Persion("aaa", 27);
        Persion p4 = new Persion("aaa", 37);
        Persion p5 = new Persion("dong", 18);

        System.out.println(p1.compareTo(p2));
        System.out.println(p2.compareTo(p3));
        System.out.println(p2.compareTo(p4));
        System.out.println(p1.compareTo(p4));
        System.out.println(p5.compareTo(p1));
    }
}
