package com.rainsoft.design.assemble;

import java.util.ArrayList;
import java.util.List;

/**
 * 假设有一个类Employee，它作为复合模式操作类。CompositePatternDemo
 * 这是一个演示类，将使用Employee类添加部门级别层次结构并打印所有员工。
 * 组合模式救命结构如下图片中所示。
 * Created by Administrator on 2017-09-07.
 */
public class Employee {
    private String name;
    private String dept;
    private int salary;
    private List<Employee> subordinates;

    public Employee(String name, String dept, int salary) {
        this.name = name;
        this.dept = dept;
        this.salary = salary;
        subordinates = new ArrayList<>();
    }

    public void add(Employee employee) {
        subordinates.add(employee);
    }

    public void remove(Employee employee) {
        subordinates.remove(employee);
    }

    public List<Employee> getSubordinates() {
        return subordinates;
    }

    @Override
    public String toString() {
        return "Employee:[" +
                "name:'" + name + '\'' +
                ", dept:'" + dept + '\'' +
                ", salary:" + salary +
                ']';
    }
}
