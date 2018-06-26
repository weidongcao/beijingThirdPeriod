package com.rainsoft.j2se;


import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by Administrator on 2017-06-15.
 */
public class ArrayTest {
    private static final Logger logger = LoggerFactory.getLogger(ArrayTest.class);
    public static void main(String[] args) throws IOException, SolrServerException {
    }

    @Test
    public void getAllSid() throws SolrServerException, IOException {
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");
        SolrClient client = (SolrClient) context.getBean("solrClient");
        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.setFilterQueries("docType:聊天");
        query.setRows(1000);

        List<String> list = new ArrayList<>();
        int num = 1000;
        for (int i = 0; i < 10000; i++) {
            query.setStart(i * num);
            QueryResponse rsp = client.query(query);
            SolrDocumentList docs = rsp.getResults();
            if (null == docs || docs.size() == 0) {
                logger.info("查询结束");
                break;
            }
            for (SolrDocument doc : docs) {
                list.add((String) doc.get("SID"));
            }
            logger.info("Solr查询了 {} 次，查出了 {} 条数据", i, docs.size());
        }
        System.out.println("list.size() = " + list.size());
        Set<String> set = new HashSet<>(list);
        System.out.println("set.size() = " + set.size());

        File file = FileUtils.getFile("D:\\0WorkSpace\\atom\\Document\\aaa.txt");
        FileUtils.writeLines(file, list);
    }

    public void getDumpSid() throws IOException {
        File file = FileUtils.getFile("D:\\0WorkSpace\\atom\\Document\\aaa.txt");
        List<String> list = FileUtils.readLines(file);
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < list.size(); i++) {
            String key = list.get(i);
            if (map.containsKey(key)) {
                map.put(key, (map.get(key) + 1));
            } else {
                map.put(key, 1);
            }
        }

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (1 < entry.getValue()) {
                System.out.println(entry.getKey() + " --> " + entry.getValue());

            }
        }

    }
}
