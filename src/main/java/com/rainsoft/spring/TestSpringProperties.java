package com.rainsoft.spring;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.PropertyPlaceholderHelper;

/**
 * Created by CaoWeiDong on 2017-07-19.
 */
public class TestSpringProperties {
    private static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

    public static void main(String[] args) {
        PropertyPlaceholderConfigurer propertyConfigurer = (PropertyPlaceholderConfigurer) context.getBean("propertyConfigurer");
        PropertyPlaceholderHelper propertyHelper = null;
    }
}
