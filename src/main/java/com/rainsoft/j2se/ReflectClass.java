package com.rainsoft.j2se;

import com.rainsoft.dao.FtpDao;
import com.rainsoft.domain.RegContentFtp;
import com.rainsoft.solr.OracleDataCreateSolrIndex;
import org.apache.solr.client.solrj.SolrServerException;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * 测试动态获取实体属性名
 * Created by CaoWeidong on 2017-06-15.
 */
public class ReflectClass {
    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException, SolrServerException {
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

        FtpDao ftpDao = (FtpDao) context.getBean("ftpDao");
        //获取数据库一天的数据
        List<RegContentFtp> datalist = ftpDao.getFtpBydate("2016-07-11");
//        datalist = datalist.subList(0,5);
        OracleDataCreateSolrIndex.writeFtpDisk(datalist);

        context.close();
    }
}
