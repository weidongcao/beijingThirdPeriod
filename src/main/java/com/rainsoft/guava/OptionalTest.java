package com.rainsoft.guava;

import com.google.common.base.Optional;
import org.junit.Test;

import java.util.Set;

/**
 * Created by CaoWeiDong on 2018-01-16.
 */
public class OptionalTest {
    public static void testNull() {
        Optional<Integer> possible = Optional.of(6);
        if (possible.isPresent()) {
            System.out.println("possible is Present: " + possible.isPresent());
            System.out.println("possible value: " + possible.get());
        }
    }


    //    @Test
    public void testOptional() {
        Optional<Integer> possible = Optional.of(6);
        Optional<Integer> absentOpt = Optional.absent();
        Optional<Integer> NullableOpt = Optional.fromNullable(null);
        Optional<Integer> NoNullableOpt = Optional.fromNullable(10);
        if (possible.isPresent()) {
            System.out.println("possible is Present: " + possible.isPresent());
            System.out.println("possible value: " + possible.get());
        }
        if (absentOpt.isPresent()) {
            System.out.println("absentOpt isPresent: " + absentOpt.isPresent());
        }
        if (NullableOpt.isPresent()) {
            System.out.println("fromNullableOpt isPresent: " + NullableOpt.isPresent());
        }
        if (NoNullableOpt.isPresent()) {
            System.out.println("NoNullableOpt isPresent: " + NoNullableOpt.isPresent());
        }
    }

    @Test
    public void testmethodReturn() {
        Optional<Long> value = method();
        if (value.isPresent() == true) {
            System.out.println("获得返回值: " + value.get());
        } else {
            System.out.println("获得返回值: " + value.or(-12L));
        }

        System.out.println("获得返回值 orNull: " + value.orNull());

        Optional<Long> valueNoNull = methodNoNull();
        if (valueNoNull.isPresent() == true) {
            Set<Long> set = valueNoNull.asSet();
            System.out.println("获得返回值set的size： " + set.size());
            System.out.println("获得返回值: " + valueNoNull.get());
        } else {
            System.out.println("获得返回值：" + valueNoNull.or(-12L));
        }


    }
    private Optional<Long> method() {
        return Optional.fromNullable(null);
    }
    private Optional<Long> methodNoNull() {
        return Optional.fromNullable(15L);
    }
}
