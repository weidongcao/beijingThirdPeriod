package com.rainsoft.utils;

import java.lang.reflect.Method;

/**
 * Created by Administrator on 2017-06-15.
 */
public class ReflectUtils {
    public static Object getFieldValueByName(String fieldName, Object o) {
        try {
            String firstLetter = fieldName.substring(0, 1).toUpperCase();
            String getter = "get" + firstLetter + fieldName.substring(1);
            Method method = o.getClass().getMethod(getter, new Class[]{});
            Object value = method.invoke(o, new Object[]{});
            return value;
        } catch (Exception e) {
            System.out.println("没有这个属性");
            return null;
        }
    }
}
