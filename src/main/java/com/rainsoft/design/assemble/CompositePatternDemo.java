package com.rainsoft.design.assemble;

/**
 * 组合模式用于需要以同样的方式处理一级对象作为单个对象。组合模式根据树结构组成对象，
 * 以表示部分以及整个层次结构。这种类型的设计模式属于结构模式，因为此模式创建了一组
 * 对象的树结构。
 * 此模式创建一个包含其自身对象的组的类。此类提供了修改其相同对象的方法
 * 我们通过以下救命展示组合模式的使用
 * Created by Administrator on 2017-09-07.
 */
public class CompositePatternDemo {
    public static void main(String[] args) {
        Employee CEO = new Employee("John", "CEO", 30000);
        Employee headSales = new Employee("Robert", "Head Sales", 2000);
        Employee headMarketing = new Employee("Michel", "Head Marketing", 2000);
        Employee clerk1 = new Employee("Laura", "Marketing", 10000);
        Employee clerk2 = new Employee("Bob", "Marketing", 10000);

        Employee salesExecutive1 = new Employee("Richard", "sales", 5000);
        Employee salesExecutive2 = new Employee("Rob", "Sales", 5000);

        CEO.add(headSales);
        CEO.add(headMarketing);

        headSales.add(salesExecutive1);
        headSales.add(salesExecutive2);

        headMarketing.add(clerk1);
        headMarketing.add(clerk2);

        //print all employees of the organization
        System.out.println("CEO = " + CEO);
        for (Employee headEmployee : CEO.getSubordinates()) {
            System.out.println("headEmployee = " + headEmployee);
            headEmployee.getSubordinates().forEach(System.out::println);
        }

    }
}
