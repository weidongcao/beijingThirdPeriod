package com.rainsoft.j2se;

import org.apache.commons.beanutils.MethodUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by CaoWeiDong on 2018-01-12.
 */
public class TestReflect {
    public static void main(String[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Map<String, Object> map = new HashMap<>();
        map.put("sid", 1);
        map.put("name", "ding");
        map.put("age", 30);
        map.put("birthday", new Date());

        Student stu = new Student();
        MethodUtils.invokeMethod(stu, "getHome", null);

        System.out.println(stu.toString());
    }

}
class Student{
    private Integer sid;
    private String name;
    private Integer age;
    private String home;

    private Date birthday;

    public Integer getSid() {
        return sid;
    }

    public void setSid(Integer sid) {
        this.sid = sid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    @Override
    public String toString() {
        return "Student{" +
                "sid=" + sid +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", home='" + home + '\'' +
                ", birthday=" + birthday +
                '}';
    }
}
