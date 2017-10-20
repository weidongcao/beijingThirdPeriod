package com.rainsoft.j2se;

import com.rainsoft.utils.DateFormatUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Created by Administrator on 2017-06-21.
 */
public class TestSolr {
    private static final Logger logger = LoggerFactory.getLogger(TestSolr.class);

    private static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

    public static SolrClient client = (SolrClient) context.getBean("solrClient");

    public static void main(String[] args)
            throws IOException, SolrServerException {
        //从Solr查询数据
        querySolr(
                "*:*"
        );
        //向Solr插入或者更新索引的例子
//        insertOrupdateSolr();

        //删除Solr的索引
//        deleteByQuery(
//                "docType:\"网页\" " +
//                "USER_NAME:\"郭晓静\" " +
//                "REF_DOMAIN:\"speed.qq.com\" " +
//                "DEST_IP:\"3722762918\""
//        );
        // 关闭连接
        client.close();
    }

    public static void insertOrupdateSolr()
            throws IOException, SolrServerException {
        //当前时间
        Date curDate = new Date();
        //生成要插入Solr的数据
        List<SolrInputDocument> docs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String rowKey = UUID.randomUUID()
                    .toString().replace("-", "")
                    + "_" + curDate.getTime();
            String captureTimeMinSecond = rowKey.split("_")[0];
            String id = rowKey.split("_")[1];
            SolrInputDocument doc = new SolrInputDocument();
            //仅有的字段
            doc.addField("ID".toUpperCase(), id);
            doc.addField("SID".toUpperCase(), rowKey);
            doc.addField("docType", "网页");
            doc.addField(
                    "IMPORT_TIME".toUpperCase()
                    , DateFormatUtils.DATE_TIME_FORMAT.format(curDate)
            );
            doc.addField("import_time", curDate.getTime());
            doc.addField(
                    "capture_time".toLowerCase()
                    , Long.valueOf(captureTimeMinSecond)
            );
            //独有的字段
            //TODO
            docs.add(doc);
        }
        //在1分钟内自动提交到Solr
        client.add(docs, 1000);
    }

    public static void deleteByQuery(String query) {
        try {
            client.deleteByQuery(query);
            client.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void querySolr(String q)
            throws IOException, SolrServerException {
        SolrQuery params = new SolrQuery();
        params.set("q", q);
        params.set("start", 0);
        params.set("rows", 5);
        QueryResponse rsp = client.query(params);
        SolrDocumentList docs = rsp.getResults();
        System.out.println("文档数量：" + docs.getNumFound());
        System.out.println("------query data:------");
        for (SolrDocument doc : docs) {
            System.out.println(doc.toString());
        }
        System.out.println("-----------------------");
    }
}