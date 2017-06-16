package com.rainsoft.j2se;

import com.rainsoft.dao.FtpDao;
import com.rainsoft.dao.HttpDao;
import com.rainsoft.domain.RegContentFtp;
import com.rainsoft.domain.RegContentHttp;
import com.rainsoft.utils.ReflectUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * 测试动态获取实体属性名
 * Created by CaoWeidong on 2017-06-15.
 */
public class ReflectClass {
    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

        HttpDao httpDao = (HttpDao) context.getBean("httpDao");
        //获取数据库一天的数据
        List<RegContentHttp> datalist = httpDao.getHttpBydate("2016-06-04");
        for (int i = 0; i < datalist.size(); i++) {
            RegContentHttp http = datalist.get(i);
            Field[] fields = RegContentHttp.class.getFields();
            for (Field field : fields) {
                String fieldName = field.getName();
                String getter = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
                Method method = http.getClass().getMethod(getter, new Class[]{});
                String fieldValue = (String) method.invoke(http, new Object[]{});
                System.out.println(fieldName + " = " + fieldValue);
            }
            System.out.println("-----------------------------------------------------------------");
        }

        context.close();
    }
}
