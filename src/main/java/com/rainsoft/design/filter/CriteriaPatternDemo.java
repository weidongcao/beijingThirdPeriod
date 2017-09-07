package com.rainsoft.design.filter;

import java.util.ArrayList;
import java.util.List;

/**
 * 过滤器模式或条件模式是一种设计模式，使开发人员可以使用不同的条件
 * 过滤一级对象,燕逻辑操作以解耦方式将其链接。这种类型的设计模式属于
 * 结构模式，因为该模式组合多个标准以获得单个标准。
 *
 * 我们将创建一个Person对象，Criteria接口和具体类，实现这个接口来过滤
 * Person对象的列表。CriteriaPatternDemo是一个演示类，使用Criteria
 * 对象基于各种标准及其组合过滤拥有Person对象的列表(List).
 * Created by Administrator on 2017-09-06.
 */
public class CriteriaPatternDemo {
    public static void main(String[] args) {
        List<Person> persons = new ArrayList<>();

        persons.add(new Person("Robert", "Male", "Single"));
        persons.add(new Person("John", "Male", "Married"));
        persons.add(new Person("Laura", "Female", "Married"));
        persons.add(new Person("Diana", "Female", "Single"));
        persons.add(new Person("Mike", "Male", "Single"));
        persons.add(new Person("Bobby", "Male", "Single"));

        Criteria male = new CriteriaMale();
        Criteria female = new CriteriaFemale();
        Criteria single = new CriteriaSingle();
        Criteria singleMale = new AndCriteria(single, male);
        Criteria singleOrFemale = new OrCriteria(single, female);

        System.out.println("Males: ");
        printPersons(male.meetCriteria(persons));

        System.out.println("\nFemales: ");
        printPersons(female.meetCriteria(persons));

        System.out.println("\nSingle Males: ");
        printPersons(singleMale.meetCriteria(persons));

        System.out.println("\nSingle Or Females: ");
        printPersons(singleOrFemale.meetCriteria(persons));
    }

    public static void printPersons(List<Person> persons) {
        for (Person person : persons) {
            System.out.println("Person : [ " +
                    "Name: " + person.getName() +
                    ", Gender: " + person.getGender() +
                    ", Marital Status: " + person.getMaritalStatus() +
                    " ]"
            );
        }
    }
}
