package com.rainsoft.j2se;

import com.rainsoft.dao.FtpDao;
import com.rainsoft.domain.RegContentFtp;
import net.sf.json.JSONObject;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by Administrator on 2017-06-21.
 */
public class TestSpringOracle {
    public static void main(String[] args) {
        testSpringConnectionOracle();
    }

    private static void testSpringConnectionOracle() {
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

        FtpDao ftpDao = (FtpDao) context.getBean("ftpDao");
        //获取数据库一天的数据
        RegContentFtp ftp = ftpDao.getFtpById(1905067);
        JSONObject jsonObject = JSONObject.fromObject(ftp);
        System.out.println(jsonObject.toString());

        context.close();
    }

}
